/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.common.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.client.MasterNotLeaderException;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.sasl.registration.RegistrationClientBootstrap;
import org.apache.celeborn.common.network.server.TransportChannelHandler;
import org.apache.celeborn.common.network.util.*;
import org.apache.celeborn.common.util.ExceptionUtils;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.Utils;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * <p>The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for all
 * TransportClients.
 *
 * <p>TransportClients will be reused whenever possible.
 */
public class TransportClientFactory implements Closeable {

  /** A simple data structure to track the pool of clients between two peer nodes. */
  private static class ClientPool {
    TransportClient[] clients;
    Object[] locks;

    ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final List<TransportClientBootstrap> clientBootstraps;
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  /** Random number generator for picking connections between peers. */
  private final Random rand;

  private final int numConnectionsPerPeer;

  private final int connectTimeoutMs;
  private final int connectionTimeoutMs;
  private final int sslHandshakeTimeoutMs;

  private final int receiveBuf;

  private final int sendBuf;
  private final Class<? extends Channel> socketChannelClass;
  private final IOMode ioMode;
  // The client worker EventLoopGroup new connections bind to. Replaced wholesale when one of its
  // loops is found dead (see recreateWorkerGroup and TransportClient#isEventLoopDead); volatile so
  // the swap is visible to concurrent callers.
  private volatile EventLoopGroup workerGroup;
  // Superseded worker groups, not shut down eagerly because their still-live threads may be
  // serving already-open channels. See retireWorkerGroupIfDrained.
  private final List<EventLoopGroup> supersededWorkerGroups = new CopyOnWriteArrayList<>();
  // Channels currently open on each worker group, keyed by group identity.
  private final ConcurrentHashMap<EventLoopGroup, Set<Channel>> workerGroupChannels;
  // Whether to recreate the worker group on a dead event loop (SPARK-58292); on by default.
  private final boolean recreateWorkerGroupOnDeadEventLoop;
  // Makes each recreated group's thread names distinct, and read outside the lock to detect that a
  // recreation happened. Mutated under the recreateWorkerGroup lock.
  private volatile int workerGroupRecreationCount;
  // Set once close() has shut the worker group down, so the dead-event-loop path does not
  // resurrect a closed factory by recreating a fresh (leaked) group. Guarded by the same lock as
  // recreateWorkerGroup.
  private boolean closed = false;
  protected ByteBufAllocator allocator;
  private final int maxClientConnectRetries;
  private final int maxClientConnectRetryWaitTimeMs;

  public TransportClientFactory(
      TransportContext context, List<TransportClientBootstrap> clientBootstraps) {
    this.context = Preconditions.checkNotNull(context);
    TransportConf conf = context.getConf();
    this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
    this.connectionPool = JavaUtils.newConcurrentHashMap();
    this.workerGroupChannels = JavaUtils.newConcurrentHashMap();
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
    this.connectTimeoutMs = conf.connectTimeoutMs();
    this.connectionTimeoutMs = conf.connectionTimeoutMs();
    this.sslHandshakeTimeoutMs = conf.sslHandshakeTimeoutMs();
    this.receiveBuf = conf.receiveBuf();
    this.sendBuf = conf.sendBuf();
    this.rand = new Random();

    this.ioMode = IOMode.valueOf(conf.ioMode());
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    logger.info("Module {} mode {} threads {}", conf.getModuleName(), ioMode, conf.clientThreads());
    this.workerGroup =
        NettyUtils.createEventLoop(
            ioMode,
            conf.clientThreads(),
            conf.conflictAvoidChooserEnable(),
            conf.getModuleName() + "-client");
    this.recreateWorkerGroupOnDeadEventLoop = conf.recreateWorkerGroupOnDeadEventLoop();
    // Always disable thread-local cache when creating pooled ByteBuf allocator for TransportClients
    // because the ByteBufs are allocated by the event loop thread, but released by the executor
    // thread rather than the event loop thread. Those thread-local caches actually delay the
    // recycling of buffers, leading to larger memory usage.
    this.allocator =
        NettyUtils.getByteBufAllocator(conf, context.getSource(), false, conf.clientThreads());
    this.maxClientConnectRetries = conf.maxIORetries();
    this.maxClientConnectRetryWaitTimeMs = conf.ioRetryWaitTimeMs();
  }

  @VisibleForTesting
  public EventLoopGroup getWorkerGroup() {
    return workerGroup;
  }

  /** How many worker groups superseded after a dead event loop have not been retired yet. */
  @VisibleForTesting
  public int supersededWorkerGroupCount() {
    return supersededWorkerGroups.size();
  }

  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   *
   * <p>We maintains an array of clients (size determined by
   * celeborn.$module.io.numConnectionsPerPeer) and randomly picks one to use. If no client was
   * previously created in the randomly selected spot, this function creates a new client and places
   * it there.
   *
   * <p>This blocks until a connection is successfully established and fully bootstrapped.
   *
   * <p>Concurrency: This method is safe to call from multiple threads.
   */
  public TransportClient createClient(String remoteHost, int remotePort, int partitionId)
      throws IOException, InterruptedException {
    return retryCreateClient(remoteHost, remotePort, partitionId, TransportFrameDecoder::new);
  }

  public TransportClient retryCreateClient(
      String remoteHost,
      int remotePort,
      int partitionId,
      Supplier<ChannelInboundHandlerAdapter> supplier)
      throws IOException, InterruptedException {
    int numTries = 0;
    while (numTries < maxClientConnectRetries) {
      try {
        return createClient(remoteHost, remotePort, partitionId, supplier.get());
      } catch (Exception e) {
        InterruptedException interruptedException = ExceptionUtils.findInterruptedException(e);
        if (interruptedException != null) {
          Thread.currentThread().interrupt();
          throw interruptedException;
        }
        numTries++;
        logger.warn(
            "Retry create client, times {}/{} with error: {}",
            numTries,
            maxClientConnectRetries,
            e.getMessage(),
            e);
        if (numTries == maxClientConnectRetries) {
          throw e;
        }

        Thread.sleep(maxClientConnectRetryWaitTimeMs);
      }
    }

    return null;
  }

  public TransportClient createClient(
      String remoteHost, int remotePort, int partitionId, ChannelInboundHandlerAdapter decoder)
      throws IOException, InterruptedException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we create a client.
    final InetSocketAddress unresolvedAddress =
        InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    ClientPool clientPool =
        connectionPool.computeIfAbsent(
            unresolvedAddress, key -> new ClientPool(numConnectionsPerPeer));
    int clientIndex =
        partitionId < 0 ? rand.nextInt(numConnectionsPerPeer) : partitionId % numConnectionsPerPeer;
    TransportClient cachedClient = clientPool.clients[clientIndex];

    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      TransportChannelHandler handler =
          cachedClient.getChannel().pipeline().get(TransportChannelHandler.class);
      if (handler != null) {
        synchronized (handler) {
          handler.getResponseHandler().updateTimeOfLastRequest();
        }
      }

      if (cachedClient.isActive()) {
        logger.debug(
            "Returning cached connection from {} to {}: {}",
            cachedClient.getChannel().localAddress(),
            cachedClient.getSocketAddress(),
            cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - preResolveHost);
    final String resolveMsg = resolvedAddress.isUnresolved() ? "failed" : "succeed";
    if (hostResolveTimeMs > 2000) {
      logger.warn(
          "DNS resolution {} for {} took {} ms", resolveMsg, resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace(
          "DNS resolution {} for {} took {} ms", resolveMsg, resolvedAddress, hostResolveTimeMs);
    }

    final int recreationCountBefore = workerGroupRecreationCount;
    try {
      synchronized (clientPool.locks[clientIndex]) {
        cachedClient = clientPool.clients[clientIndex];

        if (cachedClient != null) {
          if (cachedClient.isActive()) {
            logger.debug(
                "Returning cached connection from {} to {}: {}",
                cachedClient.getChannel().localAddress(),
                resolvedAddress,
                cachedClient);
            return cachedClient;
          } else {
            logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
          }
        }
        clientPool.clients[clientIndex] = internalCreateClient(resolvedAddress, decoder);
        return clientPool.clients[clientIndex];
      }
    } finally {
      // Runs once the pool lock above has been released: failing a client's outstanding requests
      // invokes user callbacks, which may re-enter the factory and take another pool lock, so it
      // must never happen while holding one.
      failClientsOnDeadEventLoopsIfRecreated(recreationCountBefore);
    }
  }

  public TransportClient createClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    return createClient(remoteHost, remotePort, -1);
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port. This
   * connection is not pooled.
   *
   * <p>As with {@link #createClient(String, int)}, this method is blocking.
   */
  public TransportClient createUnmanagedClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
    final int recreationCountBefore = workerGroupRecreationCount;
    try {
      return internalCreateClient(address, NettyUtils.createFrameDecoder());
    } finally {
      failClientsOnDeadEventLoopsIfRecreated(recreationCountBefore);
    }
  }

  private TransportClient internalCreateClient(
      InetSocketAddress address, ChannelInboundHandlerAdapter decoder)
      throws IOException, InterruptedException {
    return internalCreateClient(address, decoder, true);
  }

  /**
   * Connect to the given address on the current worker group.
   *
   * @param retryOnRecreatedWorkerGroup whether to immediately reconnect once if this attempt failed
   *     on a dead event loop and a fresh worker group was installed in response. That reconnect is
   *     the whole point of the recreation, so it must not be charged to the caller's I/O retry
   *     budget (celeborn.$module.io.maxRetries, which may be as low as 1, and which sleeps
   *     celeborn.$module.io.retryWait between attempts) and must also cover callers that have no
   *     retry wrapper at all, such as {@link #createUnmanagedClient}.
   */
  private TransportClient internalCreateClient(
      InetSocketAddress address,
      ChannelInboundHandlerAdapter decoder,
      boolean retryOnRecreatedWorkerGroup)
      throws IOException, InterruptedException {
    Bootstrap bootstrap = new Bootstrap();
    // Capture the group this connection uses, so that on a dead-event-loop failure we replace
    // exactly this group (and not one a concurrent caller already swapped in).
    final EventLoopGroup connectGroup = workerGroup;
    bootstrap
        .group(connectGroup)
        .channel(socketChannelClass)
        // Disable Nagle's Algorithm since we don't want packets to wait
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
        .option(ChannelOption.ALLOCATOR, allocator);

    if (receiveBuf > 0) {
      bootstrap.option(ChannelOption.SO_RCVBUF, receiveBuf);
    }

    if (sendBuf > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, sendBuf);
    }

    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();

    bootstrap.handler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) {
            TransportChannelHandler clientHandler = context.initializePipeline(ch, decoder, true);
            clientRef.set(clientHandler.getClient());
          }
        });

    // Connect to the remote server
    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);
    try {
      if (connectTimeoutMs <= 0) {
        awaitWithChannelCleanup(
            () -> {
              cf.await();
              return true;
            },
            cf);
        assert cf.isDone();
        if (cf.isCancelled()) {
          closeChannel(cf);
          throw new IOException(String.format("Connecting to %s cancelled", address));
        } else if (!cf.isSuccess()) {
          closeChannel(cf);
          throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
        }
      } else if (!awaitWithChannelCleanup(() -> cf.await(connectTimeoutMs), cf)) {
        closeChannel(cf);
        throw new CelebornIOException(
            String.format("Connecting to %s timed out (%s ms)", address, connectTimeoutMs));
      } else if (cf.cause() != null) {
        closeChannel(cf);
        throw new CelebornIOException(
            String.format("Failed to connect to %s", address), cf.cause());
      }
    } catch (IOException e) {
      // Registration may have been rejected because the loop it landed on is dead, which degrades
      // the whole group permanently. Replace it, then reconnect straight away so the request that
      // triggered the recovery is the first to benefit from it rather than the one that pays.
      if (recreateWorkerGroupIfEventLoopDead(connectGroup, cf.cause())
          && retryOnRecreatedWorkerGroup) {
        logger.warn("Retrying the connection to {} on a fresh worker group", address, e);
        // Reusing `decoder` is safe here: the dead loop rejected the channel registration, so the
        // ChannelInitializer above never ran and the decoder was never added to a pipeline.
        return internalCreateClient(address, decoder, false);
      }
      throw e;
    }
    trackChannel(connectGroup, cf.channel());
    if (context.sslEncryptionEnabled()) {
      final SslHandler sslHandler = cf.channel().pipeline().get(SslHandler.class);
      sslHandler.setHandshakeTimeoutMillis(sslHandshakeTimeoutMs);
      Future<Channel> future =
          sslHandler
              .handshakeFuture()
              .addListener(
                  new GenericFutureListener<Future<Channel>>() {
                    @Override
                    public void operationComplete(final Future<Channel> handshakeFuture) {
                      if (handshakeFuture.isSuccess()) {
                        logger.debug("successfully completed TLS handshake to {}", address);
                      } else {
                        logger.info(
                            "failed to complete TLS handshake to {}",
                            address,
                            handshakeFuture.cause());
                        closeChannel(cf);
                      }
                    }
                  });
      if (!awaitWithChannelCleanup(() -> future.await(connectionTimeoutMs), cf)) {
        closeChannel(cf);
        throw new IOException(
            String.format("Failed to connect to %s within connection timeout", address));
      }
    }

    TransportClient client = clientRef.get();
    assert client != null : "Channel future completed successfully with null client";

    // Execute any client bootstraps synchronously before marking the Client as successful.
    long preBootstrap = System.nanoTime();
    logger.debug("Running bootstraps for {} ...", address);
    for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
      try {
        clientBootstrap.doBootstrap(client);
      } catch (
          Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
        long bootstrapTime = System.nanoTime() - preBootstrap;
        if (clientBootstrap instanceof RegistrationClientBootstrap) {
          Exception processed = RegistrationClientBootstrap.processMasterNotLeaderException(e);
          String message =
              (processed instanceof MasterNotLeaderException)
                  ? String.format(
                      "Suggested leader is %s",
                      ((MasterNotLeaderException) processed).getSuggestedLeaderAddress())
                  : e.getMessage();
          logger.warn(
              "Attempted to register with a Master that is not the leader after {}: {}",
              Utils.nanoDurationToString(bootstrapTime),
              message);
        } else {
          logger.error(
              "Exception while bootstrapping client after {}",
              Utils.nanoDurationToString(bootstrapTime),
              e);
        }
        client.close();
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }
    long postBootstrap = System.nanoTime();
    logger.debug(
        "Successfully created connection to {} after {} ({} spent in bootstraps)",
        address,
        Utils.nanoDurationToString(postBootstrap - preConnect),
        Utils.nanoDurationToString(postBootstrap - preBootstrap));

    return client;
  }

  @FunctionalInterface
  @VisibleForTesting
  interface InterruptibleAwait {
    boolean await() throws InterruptedException;
  }

  @VisibleForTesting
  static boolean awaitWithChannelCleanup(
      InterruptibleAwait interruptibleAwait, ChannelFuture channelFuture)
      throws InterruptedException {
    try {
      return interruptibleAwait.await();
    } catch (InterruptedException e) {
      closeChannel(channelFuture);
      Thread.currentThread().interrupt();
      throw e;
    }
  }

  /**
   * If the given connection-failure cause was a rejection by a dead netty event loop (its worker
   * thread terminated and netty rejects new registrations with a {@link
   * RejectedExecutionException}), replace the worker group so subsequent connections bind to fresh,
   * live threads. Without this the degradation is permanent - see {@link
   * TransportClient#isEventLoopDead()}.
   *
   * @return whether the caller may now retry: either this call replaced the group, or a concurrent
   *     caller already did and the current group is therefore a fresh one.
   */
  private boolean recreateWorkerGroupIfEventLoopDead(EventLoopGroup connectGroup, Throwable cause) {
    if (!recreateWorkerGroupOnDeadEventLoop) {
      return false;
    }
    boolean eventLoopDead = false;
    for (Throwable t = cause; t != null; t = t.getCause()) {
      // Match ONLY the terminated-loop rejection, not a transient task-queue-full rejection.
      // netty's SingleThreadEventExecutor.reject() throws exactly this message when isShutdown();
      // the queue-full handler path throws a RejectedExecutionException with no message.
      if (t instanceof RejectedExecutionException
          && "event executor terminated".equals(t.getMessage())) {
        eventLoopDead = true;
        break;
      }
    }
    return eventLoopDead && recreateWorkerGroup(connectGroup);
  }

  /**
   * Replace the worker group with a fresh one, if it is still the group the failed connection used
   * ({@code connectGroup}). The superseded group is not shut down here: its still-live threads may
   * be serving already-open channels, so it is retired by {@link #retireWorkerGroupIfDrained}
   * instead. Synchronized and identity-guarded so concurrent callers that all hit the same dead
   * group replace it exactly once rather than spawning many groups.
   *
   * @return whether a fresh group is now installed and the caller may retry on it.
   */
  private synchronized boolean recreateWorkerGroup(EventLoopGroup connectGroup) {
    // The factory is closed (or closing): its worker group was shut down by close(), so a
    // createClient() racing or following close() must not recreate a fresh group and resurrect a
    // closed factory (which would leak threads that close() will never reap again).
    if (closed) {
      return false;
    }
    // A concurrent caller that hit the same dead group already swapped it out. Nothing to do, but
    // the current group is a fresh one, so the caller can still retry on it.
    if (workerGroup != connectGroup) {
      return true;
    }
    // Tag the thread names so a dead-event-loop recovery is obvious in a thread dump, and so
    // successive recreations stay distinguishable if a loop dies more than once.
    workerGroupRecreationCount++;
    TransportConf conf = context.getConf();
    String threadPrefix = conf.getModuleName() + "-client-recreated-" + workerGroupRecreationCount;
    workerGroup =
        NettyUtils.createEventLoop(
            ioMode, conf.clientThreads(), conf.conflictAvoidChooserEnable(), threadPrefix);
    supersededWorkerGroups.add(connectGroup);
    logger.warn(
        "Detected a dead netty event loop in the {} client worker group; replaced it with {}. "
            + "The superseded group keeps serving its {} already-open channels until they drain "
            + "(SPARK-58292).",
        conf.getModuleName(),
        threadPrefix,
        channelCount(connectGroup));
    // If it has no channels left, nothing will ever untrack one on its behalf. Check once, here.
    retireWorkerGroupIfDrained(connectGroup);
    return true;
  }

  /**
   * Register a newly connected channel against the worker group it is pinned to. Tracking exists
   * solely so a superseded group can be retired, which cannot happen unless recreation is enabled,
   * so skip the bookkeeping entirely when it is off.
   */
  private void trackChannel(EventLoopGroup group, Channel channel) {
    if (!recreateWorkerGroupOnDeadEventLoop) {
      return;
    }
    workerGroupChannels
        .computeIfAbsent(group, unused -> ConcurrentHashMap.newKeySet())
        .add(channel);
    channel.closeFuture().addListener(future -> untrackChannel(group, channel));
  }

  /** Called from the channel's close future, and from {@link #failClientsOnDeadEventLoops()}. */
  private void untrackChannel(EventLoopGroup group, Channel channel) {
    Set<Channel> channels = workerGroupChannels.get(group);
    if (channels != null) {
      channels.remove(channel);
    }
    retireWorkerGroupIfDrained(group);
  }

  /**
   * Shut down a superseded worker group once it has no channels left to serve. It cannot simply be
   * dropped and left to the GC: a netty thread keeps its executor, and the executor its parent
   * group, strongly reachable. So without this, one dead event loop would cost the process the
   * group's other clientThreads() - 1 selector threads for the lifetime of the factory, and
   * repeated recoveries would accumulate them.
   *
   * <p>Best-effort: a connection that captured this group before it was superseded may still
   * register a channel afterwards, but such a connection is failing anyway, and {@link #close()}
   * remains the backstop for any group that never drains.
   */
  private synchronized void retireWorkerGroupIfDrained(EventLoopGroup group) {
    if (closed || group == workerGroup) {
      return;
    }
    Set<Channel> channels = workerGroupChannels.get(group);
    if (channels != null && !channels.isEmpty()) {
      return;
    }
    workerGroupChannels.remove(group);
    if (supersededWorkerGroups.remove(group) && !group.isShuttingDown()) {
      logger.info(
          "A superseded {} client worker group has drained; shutting it down. {} superseded "
              + "group(s) still retained.",
          context.getConf().getModuleName(),
          supersededWorkerGroups.size());
      group.shutdownGracefully();
    }
  }

  /** Number of channels currently tracked as open on the given worker group. */
  private int channelCount(EventLoopGroup group) {
    Set<Channel> channels = workerGroupChannels.get(group);
    return channels == null ? 0 : channels.size();
  }

  /** Sweep only if the worker group has been recreated since {@code recreationCountBefore}. */
  private void failClientsOnDeadEventLoopsIfRecreated(int recreationCountBefore) {
    if (workerGroupRecreationCount == recreationCountBefore) {
      return;
    }
    try {
      failClientsOnDeadEventLoops();
    } catch (Throwable t) {
      // Never let this mask the outcome of the createClient call it is attached to.
      logger.warn("Error while invalidating clients pinned to a dead netty event loop", t);
    }
  }

  /**
   * Synchronously fail the outstanding requests of every pooled client pinned to a dead event loop.
   * Marking such a client inactive stops the factory handing it out again, but does nothing for
   * whoever already holds it: an owner that keeps a client for the lifetime of a stream - e.g.
   * Flink's {@code CelebornBufferStream}, which sends credits on the client it captured rather than
   * reacquiring one - would otherwise wait forever. Failing its callbacks is the only way it can
   * notice; the client cannot be force-closed. See {@link TransportClient#isEventLoopDead()}.
   *
   * <p>MUST be called with no pool lock held: failing a request invokes its callback on this
   * thread, and callbacks re-enter the factory.
   *
   * <p>Only pooled clients are reachable from here, so a client handed out by {@link
   * #createUnmanagedClient} is left to its owner to invalidate via {@link
   * TransportClient#invalidateIfEventLoopDead()}.
   */
  @VisibleForTesting
  public void failClientsOnDeadEventLoops() {
    for (ClientPool clientPool : connectionPool.values()) {
      // Read without the pool lock: this is a best-effort sweep, and taking the lock here would
      // invert the pool-then-factory lock order that createClient establishes.
      for (TransportClient client : clientPool.clients) {
        if (client == null || !client.isEventLoopDead()) {
          continue;
        }
        client.invalidateIfEventLoopDead();
        // A dead loop never completes the close future, so untrack here or the group never retires.
        Channel channel = client.getChannel();
        untrackChannel(channel.eventLoop().parent(), channel);
      }
    }
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    // Mark closed under the recreateWorkerGroup lock before shutting the group down, so a
    // concurrent createClient() hitting the dead-event-loop path cannot recreate a fresh group
    // after we have decided to close (which would leak threads).
    synchronized (this) {
      closed = true;
    }

    // SPARK-19147
    if (workerGroup != null && !workerGroup.isShuttingDown()) {
      workerGroup.shutdownGracefully();
    }

    // Backstop for worker groups superseded after a dead-event-loop recreation whose channels
    // never drained, so retireWorkerGroupIfDrained could not shut them down earlier.
    for (EventLoopGroup group : supersededWorkerGroups) {
      if (!group.isShuttingDown()) {
        group.shutdownGracefully();
      }
    }
    supersededWorkerGroups.clear();
    workerGroupChannels.clear();
  }

  public TransportContext getContext() {
    return context;
  }

  private static void closeChannel(ChannelFuture channelFuture) {
    try {
      channelFuture.channel().close();
    } catch (Exception e) {
      logger.warn("Failed to close channel", e);
    }
  }
}
