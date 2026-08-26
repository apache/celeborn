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

package org.apache.celeborn.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.compress.Compressor;
import org.apache.celeborn.client.read.CelebornInputStream;
import org.apache.celeborn.client.read.MetricsCallback;
import org.apache.celeborn.client.security.CryptoHandler;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornBroadcastException;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.exception.CelebornRuntimeException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.metrics.source.Role;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.*;
import org.apache.celeborn.common.network.protocol.*;
import org.apache.celeborn.common.network.protocol.SerdeVersion;
import org.apache.celeborn.common.network.sasl.SaslClientBootstrap;
import org.apache.celeborn.common.network.sasl.SaslCredentials;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.*;
import org.apache.celeborn.common.protocol.message.ControlMessages.*;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.rpc.RpcAddress;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.*;
import org.apache.celeborn.common.write.DataBatches;
import org.apache.celeborn.common.write.LocationPushFailedBatches;
import org.apache.celeborn.common.write.PushState;

public class ShuffleClientImpl extends ShuffleClient {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleClientImpl.class);

  protected static final byte PRIMARY_MODE = PartitionLocation.Mode.PRIMARY.mode();

  private static final Random RND = new Random();

  protected final CelebornConf conf;

  private final UserIdentifier userIdentifier;

  private final int registerShuffleMaxRetries;
  private final long registerShuffleRetryWaitMs;
  private final int rpcMaxRetries;
  private final long rpcRetryWait;
  private final int maxReviveTimes;
  private final boolean testRetryRevive;
  private final int pushBufferMaxSize;
  protected final long pushDataTimeout;

  private final RpcEnv rpcEnv;

  protected RpcEndpointRef lifecycleManagerRef;

  private TransportContext transportContext;
  protected TransportClientFactory dataClientFactory;

  protected final int BATCH_HEADER_SIZE = 4 * 4;

  protected byte[] extension;

  private volatile Optional<CryptoHandler> cryptoHandler = Optional.empty();

  // key: appShuffleIdentifier, value: shuffleId
  protected Map<String, Tuple2<Integer, Boolean>> shuffleIdCache = JavaUtils.newConcurrentHashMap();

  // key: shuffleId, value: (partitionId, PartitionLocationGroup)
  final Map<Integer, ConcurrentHashMap<Integer, PartitionLocationGroup>> reducePartitionMap =
      JavaUtils.newConcurrentHashMap();

  // key: shuffleId, value: Set(mapId)
  protected final ConcurrentHashMap<Integer, Set<Integer>> mapperEndMap =
      JavaUtils.newConcurrentHashMap();

  // key: mapKey (shuffleId-mapId-attemptId), value: (worker hostAndPushPort -> counters)
  // Aggregated counts of push events (soft/hard split, congested) per map task,
  // logged and cleared when the map task ends.
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, PushEventCounters>>
      pushEventStats = JavaUtils.newConcurrentHashMap();

  private final long slowPushThresholdNanos;

  // shuffleIds which have finished all map tasks
  protected final Set<Integer> stageEndShuffleSet = ConcurrentHashMap.newKeySet();

  // key: shuffleId-mapId-attemptId
  protected final Map<String, PushState> pushStates = JavaUtils.newConcurrentHashMap();

  private final boolean pushExcludeWorkerOnFailureEnabled;
  private final boolean shuffleCompressionEnabled;
  protected final boolean shuffleIntegrityCheckEnabled;

  private final Set<String> pushExcludedWorkers = ConcurrentHashMap.newKeySet();
  private final ConcurrentHashMap<String, Long> fetchExcludedWorkers =
      JavaUtils.newConcurrentHashMap();
  private boolean pushReplicateEnabled;
  private boolean fetchExcludeWorkerOnFailureEnabled;

  private final ExecutorService pushDataRetryPool;

  private final Map<Integer, Set<Integer>> splitting = JavaUtils.newConcurrentHashMap();

  protected final String appUniqueId;
  private final boolean authEnabled;
  private final TransportConf dataTransportConf;

  @SuppressWarnings("ThreadLocalUsage")
  private final ThreadLocal<Compressor> compressorThreadLocal =
      new ThreadLocal<Compressor>() {
        @Override
        protected Compressor initialValue() {
          return Compressor.getCompressor(conf);
        }
      };

  private final ReviveManager reviveManager;

  private final boolean dataPushFailureTrackingEnabled;

  // Package-private: ReviveManager reads it to decide whether satisfied retire reports
  // still have to be forwarded to the LifecycleManager.
  final boolean adaptivePartitionWriteParallelismEnabled;

  public static class ReduceFileGroups {
    public Map<Integer, Set<PartitionLocation>> partitionGroups;
    public Map<String, LocationPushFailedBatches> pushFailedBatches;
    public int[] mapAttempts;
    public Set<Integer> partitionIds;

    ReduceFileGroups(
        Map<Integer, Set<PartitionLocation>> partitionGroups,
        int[] mapAttempts,
        Set<Integer> partitionIds,
        Map<String, LocationPushFailedBatches> pushFailedBatches) {
      this.partitionGroups = partitionGroups;
      this.mapAttempts = mapAttempts;
      this.partitionIds = partitionIds;
      this.pushFailedBatches = pushFailedBatches;
    }

    public ReduceFileGroups() {
      this.partitionGroups = null;
      this.mapAttempts = null;
      this.partitionIds = null;
      this.pushFailedBatches = null;
    }

    public void update(ReduceFileGroups fileGroups) {
      partitionGroups = fileGroups.partitionGroups;
      mapAttempts = fileGroups.mapAttempts;
      partitionIds = fileGroups.partitionIds;
      pushFailedBatches = fileGroups.pushFailedBatches;
    }
  }

  // key: shuffleId
  protected final Map<Integer, Tuple3<ReduceFileGroups, String, Exception>> reduceFileGroupsMap =
      JavaUtils.newConcurrentHashMap();

  // Per-shuffle monitor serializing first-time file group loads, so the blocking
  // GetReducerFileGroup RPC runs outside reduceFileGroupsMap's bin lock (see updateFileGroup).
  private final Map<Integer, Object> fileGroupLoadLocks = JavaUtils.newConcurrentHashMap();

  private final TransportMessagesHelper messagesHelper = new TransportMessagesHelper();

  public ShuffleClientImpl(String appUniqueId, CelebornConf conf, UserIdentifier userIdentifier) {
    super();
    this.appUniqueId = appUniqueId;
    this.conf = conf;
    this.userIdentifier = userIdentifier;
    registerShuffleMaxRetries = conf.clientRegisterShuffleMaxRetry();
    registerShuffleRetryWaitMs = conf.clientRegisterShuffleRetryWaitMs();
    rpcMaxRetries = conf.clientRpcMaxRetries();
    rpcRetryWait = conf.clientRpcRetryWait();
    maxReviveTimes = conf.clientPushMaxReviveTimes();
    testRetryRevive = conf.testRetryRevive();
    pushBufferMaxSize = conf.clientPushBufferMaxSize();
    pushExcludeWorkerOnFailureEnabled = conf.clientPushExcludeWorkerOnFailureEnabled();
    shuffleCompressionEnabled = !conf.shuffleCompressionCodec().equals(CompressionCodec.NONE);
    pushReplicateEnabled = conf.clientPushReplicateEnabled();
    fetchExcludeWorkerOnFailureEnabled = conf.clientFetchExcludeWorkerOnFailureEnabled();
    shuffleIntegrityCheckEnabled = conf.clientShuffleIntegrityCheckEnabled();
    if (conf.clientPushReplicateEnabled()) {
      pushDataTimeout = conf.pushDataTimeoutMs() * 2;
    } else {
      pushDataTimeout = conf.pushDataTimeoutMs();
    }
    authEnabled = conf.authEnabledOnClient();
    dataPushFailureTrackingEnabled = conf.clientAdaptiveOptimizeSkewedPartitionReadEnabled();
    adaptivePartitionWriteParallelismEnabled =
        conf.clientShuffleAdaptivePartitionWriteParallelismEnabled();
    slowPushThresholdNanos = TimeUnit.MILLISECONDS.toNanos(conf.clientPushSlowPushThresholdMs());

    // init rpc env
    rpcEnv =
        RpcEnv.create(
            RpcNameConstants.SHUFFLE_CLIENT_SYS,
            TransportModuleConstants.RPC_APP_CLIENT_MODULE,
            Utils.localHostName(conf),
            0,
            conf,
            Role.CLIENT(),
            scala.None$.empty());

    String module = TransportModuleConstants.DATA_MODULE;
    dataTransportConf = Utils.fromCelebornConf(conf, module, conf.networkIoThreads(module));
    initDataClientFactoryIfNeeded();
    int pushDataRetryThreads = conf.clientPushRetryThreads();
    pushDataRetryPool =
        ThreadUtils.newDaemonCachedThreadPool("celeborn-retry-sender", pushDataRetryThreads, 60);

    reviveManager = new ReviveManager(this, conf);

    if (conf.hasS3Storage()) {
      Map<StorageInfo.Type, FileSystem> hadoopFs = getHadoopFs(conf);
      FileSystem s3client = hadoopFs.get(StorageInfo.Type.S3);
      logger.info("S3 client: {}", s3client);
      if (s3client == null)
        throw new IllegalStateException("S3 type is requred but the S3 client was not created");
    }
    logger.info("Created ShuffleClientImpl, appUniqueId: {}", appUniqueId);
  }

  protected List<TransportClientBootstrap> createBootstraps() {
    if (authEnabled && null != lifecycleManagerRef) {
      PbApplicationMetaRequest pbApplicationMetaRequest =
          PbApplicationMetaRequest.newBuilder().setAppId(appUniqueId).build();
      PbApplicationMeta pbApplicationMeta =
          lifecycleManagerRef.askSync(
              pbApplicationMetaRequest,
              conf.clientRpcRegisterShuffleAskTimeout(),
              ClassTag$.MODULE$.apply(PbApplicationMeta.class));
      List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
      bootstraps.add(
          new SaslClientBootstrap(
              dataTransportConf,
              appUniqueId,
              new SaslCredentials(appUniqueId, pbApplicationMeta.getSecret())));
      return Collections.unmodifiableList(bootstraps);
    } else {
      return Collections.emptyList();
    }
  }

  private void initDataClientFactoryIfNeeded() {
    if (dataClientFactory != null) {
      return;
    }
    this.transportContext =
        new TransportContext(
            dataTransportConf, new BaseMessageHandler(), conf.clientCloseIdleConnections());
    if (!authEnabled) {
      logger.info("Initializing data client factory for {}.", appUniqueId);
      dataClientFactory = transportContext.createClientFactory();
    } else if (lifecycleManagerRef != null) {
      logger.info("Initializing data client factory for secured {}.", appUniqueId);
      List<TransportClientBootstrap> bootstraps = createBootstraps();
      dataClientFactory = transportContext.createClientFactory(bootstraps);
    }
  }

  private boolean isPushTargetWorkerExcluded(
      PartitionLocation location, RpcResponseCallback wrappedCallback) {
    // If pushExcludeWorkerOnFailureEnabled = false, pushExcludedWorkers should be empty.
    if (pushExcludedWorkers.contains(location.hostAndPushPort())) {
      wrappedCallback.onFailure(
          new CelebornIOException(StatusCode.PUSH_DATA_PRIMARY_WORKER_EXCLUDED));
      return true;
    } else if (location.hasPeer()
        && pushExcludedWorkers.contains(location.getPeer().hostAndPushPort())) {
      wrappedCallback.onFailure(
          new CelebornIOException(StatusCode.PUSH_DATA_REPLICA_WORKER_EXCLUDED));
      return true;
    } else {
      return false;
    }
  }

  private void submitRetryPushData(
      int shuffleId,
      byte[] body,
      int batchId,
      PushDataRpcResponseCallback pushDataRpcResponseCallback,
      PushState pushState,
      ReviveRequest request,
      int remainReviveTimes,
      long dueTime) {
    int mapId = request.mapId;
    int attemptId = request.attemptId;
    PartitionLocation loc = request.loc;
    StatusCode cause = request.cause;
    int partitionId = loc.getId();
    long reviveWaitTime = dueTime - System.currentTimeMillis();
    final long delta = 50;
    long accumulatedTime = 0;
    while (request.reviveStatus == StatusCode.REVIVE_INITIALIZED.getValue()
        && accumulatedTime <= reviveWaitTime) {
      try {
        Thread.sleep(delta);
        accumulatedTime += delta;
      } catch (InterruptedException e) {
        logger.error("Interrupted while waiting for Revive result!");
        Thread.currentThread().interrupt();
      }
    }
    if (mapperEnded(shuffleId, mapId)) {
      logger.debug(
          "Revive for push data success, but the mapper already ended for shuffle {} map {} attempt {} partition {} batch {} location {}.",
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          batchId,
          loc);
      pushState.removeBatch(batchId, loc.hostAndPushPort());
    } else if (request.reviveStatus != StatusCode.SUCCESS.getValue()) {
      logger.warn(
          "Revive for push data failed after waiting {} ms for shuffle {} map {} attempt {} partition {} batch {}: "
              + "cause {}, revive status {}({}), old location {}.",
          accumulatedTime,
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          batchId,
          cause,
          request.reviveStatus,
          StatusCode.fromValue(request.reviveStatus),
          request.loc);
      pushDataRpcResponseCallback.onFailure(
          new CelebornIOException(
              cause
                  + " then revive but "
                  + StatusCode.REVIVE_FAILED
                  + ", revive status "
                  + request.reviveStatus
                  + "("
                  + StatusCode.fromValue(request.reviveStatus)
                  + ")"
                  + ", old location: "
                  + request.loc));
    } else {
      PartitionLocationGroup newLocGroup = locationGroup(shuffleId, partitionId);
      PartitionLocation newLoc = newLocGroup == null ? null : newLocGroup.currentFor(mapId);
      if (newLoc == null && newLocGroup != null && adaptivePartitionWriteParallelismEnabled) {
        // Revive reported SUCCESS but every known location is locally retired — the LM's active
        // set is stale (its digest of the retire reports lags). Do a bounded blocking revive
        // with all outstanding retire reports attached instead of failing the task.
        newLoc =
            reviveUntilUsable(
                shuffleId, mapId, attemptId, partitionId, newLocGroup, /* maxAttempts= */ 3);
      }
      if (newLoc == null) {
        pushDataRpcResponseCallback.onFailure(
            new CelebornIOException(
                cause
                    + " then revive but no usable location for partition "
                    + partitionId
                    + (newLocGroup == null
                        ? ""
                        : "; active epochs: "
                            + newLocGroup.activeEpochsSnapshot()
                            + ", retired: "
                            + newLocGroup.retiredEpochsSnapshot())));
        return;
      }
      // One line per re-pushed batch: a churny partition (frequent split/retire) retries many
      // batches per second, which floods the executor log at INFO.
      logger.debug(
          "Revive for push data success after waiting {} ms, new location for shuffle {} map {} attempt {} partition {} batch {} is location {}.",
          accumulatedTime,
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          batchId,
          newLoc);
      pushDataRpcResponseCallback.updateLatestPartition(newLoc);
      try {
        if (!isPushTargetWorkerExcluded(newLoc, pushDataRpcResponseCallback)) {
          if (!testRetryRevive || remainReviveTimes < 1) {
            assert dataClientFactory != null;
            TransportClient client =
                dataClientFactory.createClient(newLoc.getHost(), newLoc.getPushPort(), partitionId);
            NettyManagedBuffer newBuffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(body));
            String shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId);
            PushData newPushData =
                new PushData(PRIMARY_MODE, shuffleKey, newLoc.getUniqueId(), newBuffer);
            client.pushData(newPushData, pushDataTimeout, pushDataRpcResponseCallback);
          } else {
            throw new RuntimeException(
                "Mock push data submit retry failed. remainReviveTimes = "
                    + remainReviveTimes
                    + ".");
          }
        }
      } catch (Exception e) {
        logger.error(
            "Exception raised while pushing data for shuffle {} map {} attempt {} partition {} batch {} location {}.",
            shuffleId,
            mapId,
            attemptId,
            partitionId,
            batchId,
            newLoc,
            e);
        if (e instanceof InterruptedException) {
          pushDataRpcResponseCallback.onFailure(e);
        } else {
          pushDataRpcResponseCallback.onFailure(
              new CelebornIOException(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY, e));
        }
      }
    }
  }

  public ReviveRequest[] addAndGetReviveRequests(
      int shuffleId,
      int mapId,
      int attemptId,
      ArrayList<DataBatches.DataBatch> batches,
      StatusCode cause) {
    ReviveRequest[] reviveRequests = new ReviveRequest[batches.size()];
    for (int i = 0; i < batches.size(); i++) {
      DataBatches.DataBatch batch = batches.get(i);
      PartitionLocation loc = batch.loc;
      ReviveRequest reviveRequest =
          new ReviveRequest(shuffleId, mapId, attemptId, loc.getId(), loc.getEpoch(), loc, cause);
      reviveManager.addRequest(reviveRequest);
      reviveRequests[i] = reviveRequest;
    }
    return reviveRequests;
  }

  private PartitionLocationGroup locationGroup(int shuffleId, int partitionId) {
    ConcurrentHashMap<Integer, PartitionLocationGroup> partitionMap =
        reducePartitionMap.get(shuffleId);
    return partitionMap == null ? null : partitionMap.get(partitionId);
  }

  /**
   * For adaptive parallel write: retire the failed epochs locally, and for partitions that still
   * have another active location, preset the revive request status to SUCCESS so that the retry
   * thread re-pushes to the other active location immediately instead of waiting for revive.
   */
  private void presetSuccessIfAnotherUsable(
      int shuffleId, int mapId, ReviveRequest[] requests, StatusCode cause) {
    if (!adaptivePartitionWriteParallelismEnabled) {
      return;
    }
    for (ReviveRequest request : requests) {
      PartitionLocationGroup group = locationGroup(shuffleId, request.partitionId);
      if (group != null) {
        retireAndPresetIfAnotherUsable(shuffleId, mapId, group, request.epoch, cause, request);
      }
    }
  }

  /**
   * Retire the epoch in the location group and, when the partition still has another active
   * location for this map, preset the revive request status to SUCCESS so that the retry thread
   * re-pushes to the other active location immediately instead of waiting for revive.
   */
  private void retireAndPresetIfAnotherUsable(
      int shuffleId,
      int mapId,
      PartitionLocationGroup group,
      int epoch,
      StatusCode cause,
      ReviveRequest request) {
    boolean firstRetire = group.retire(epoch, cause);
    PartitionLocation fallback = group.anotherUsableFor(mapId, epoch);
    if (fallback != null) {
      // Another active location is available: re-push to it immediately without waiting for
      // the revive response. The retry thread reads the preset SUCCESS and picks the other
      // active location.
      request.reviveStatus = StatusCode.SUCCESS.getValue();
      // Log only on the first retire of the epoch: every batch that gets the split response
      // lands here, so logging unconditionally floods the executor log.
      if (firstRetire) {
        logger.info(
            "Shuffle {} partition {}: location epoch {} retired (cause {}), "
                + "re-push immediately to epoch {}@{} without waiting for revive.",
            shuffleId,
            request.partitionId,
            epoch,
            cause,
            fallback.getEpoch(),
            fallback.hostAndPushPort());
      }
    } else if (firstRetire) {
      logger.info(
          "Shuffle {} partition {}: location epoch {} retired (cause {}), "
              + "no other usable location, waiting for revive.",
          shuffleId,
          request.partitionId,
          epoch,
          cause);
    }
  }

  /**
   * Handle a SOFT_SPLIT of the given location: mark the epoch soft-split in the location group (it
   * stays writable and keeps receiving its share of writes until it hard-splits) and, on the first
   * retire of the epoch, report it to the LifecycleManager for hotness judgment (boosting the
   * location count when the partition is hot). Data already landed on the worker, so writes are
   * never blocked.
   */
  private void handleSoftSplitRetire(
      int shuffleId, int mapId, int attemptId, int partitionId, PartitionLocation latest) {
    if (adaptivePartitionWriteParallelismEnabled) {
      PartitionLocationGroup latestGroup = locationGroup(shuffleId, partitionId);
      if (latestGroup != null) {
        boolean newlyRetired = latestGroup.retire(latest.getEpoch(), StatusCode.SOFT_SPLIT);
        if (newlyRetired) {
          logger.info(
              "Shuffle {} partition {}: location epoch {}@{} soft-split, "
                  + "remains writable for routing and report to LifecycleManager.",
              shuffleId,
              partitionId,
              latest.getEpoch(),
              latest.hostAndPushPort());
        }
        if (newlyRetired && !mapperEnded(shuffleId, mapId)) {
          ReviveRequest reviveRequest =
              new ReviveRequest(
                  shuffleId,
                  mapId,
                  attemptId,
                  partitionId,
                  latest.getEpoch(),
                  latest,
                  StatusCode.SOFT_SPLIT);
          reviveManager.addRequest(reviveRequest);
        }
      }
    } else if (!newerPartitionLocationExists(
        reducePartitionMap.get(shuffleId), partitionId, latest.getEpoch(), false)) {
      ReviveRequest reviveRequest =
          new ReviveRequest(
              shuffleId,
              mapId,
              attemptId,
              partitionId,
              latest.getEpoch(),
              latest,
              StatusCode.SOFT_SPLIT);
      reviveManager.addRequest(reviveRequest);
    }
  }

  private void submitRetryPushMergedData(
      PushState pushState,
      int shuffleId,
      int mapId,
      int attemptId,
      ArrayList<DataBatches.DataBatch> batches,
      StatusCode cause,
      Integer oldGroupedBatchId,
      ReviveRequest[] reviveRequests,
      int remainReviveTimes,
      long reviveResponseDueTime) {
    HashMap<Pair<String, String>, DataBatches> newDataBatchesMap = new HashMap<>();
    ArrayList<DataBatches.DataBatch> reviveFailedBatchesMap = new ArrayList<>();

    long reviveWaitTime = reviveResponseDueTime - System.currentTimeMillis();
    final long delta = 50;
    long accumulatedTime = 0;
    int index = 0;
    while (index < reviveRequests.length && accumulatedTime <= reviveWaitTime) {
      ReviveRequest request = reviveRequests[index];
      DataBatches.DataBatch batch = batches.get(index);
      if (request.reviveStatus != StatusCode.REVIVE_INITIALIZED.getValue()) {
        if (mapperEnded(shuffleId, mapId)) {
          logger.debug(
              "Revive for push merged data success, but the mapper already ended for shuffle {} map {} attempt {} partition {} batch {}.",
              shuffleId,
              mapId,
              attemptId,
              request.partitionId,
              oldGroupedBatchId);
        } else if (request.reviveStatus == StatusCode.SUCCESS.getValue()) {
          PartitionLocationGroup newLocGroup = locationGroup(shuffleId, request.partitionId);
          PartitionLocation newLoc = newLocGroup == null ? null : newLocGroup.currentFor(mapId);
          if (newLoc != null) {
            DataBatches newDataBatches =
                newDataBatchesMap.computeIfAbsent(genAddressPair(newLoc), (s) -> new DataBatches());
            newDataBatches.addDataBatch(newLoc, batch.batchId, batch.body);
          } else if (remainReviveTimes > 0) {
            reviveFailedBatchesMap.add(batch);
          } else {
            String errorMsg =
                String.format(
                    "Revive succeeded but no usable location while pushing merged for shuffle %d map %d attempt %d partition %d batch %d location %s.",
                    shuffleId, mapId, attemptId, request.partitionId, oldGroupedBatchId, batch.loc);
            pushState.exception.compareAndSet(null, new CelebornIOException(errorMsg));
            return;
          }
        } else {
          if (remainReviveTimes > 0) {
            reviveFailedBatchesMap.add(batch);
          } else {
            String errorMsg =
                String.format(
                    "Revive failed while pushing merged for shuffle %d map %d attempt %d partition %d batch %d location %s.",
                    shuffleId, mapId, attemptId, request.partitionId, oldGroupedBatchId, batch.loc);
            pushState.exception.compareAndSet(
                null,
                new CelebornIOException(
                    errorMsg,
                    new CelebornIOException(
                        cause
                            + " then revive but "
                            + request.reviveStatus
                            + "("
                            + StatusCode.fromValue(request.reviveStatus)
                            + ")")));
            return;
          }
        }
        index++;
      } else {
        try {
          Thread.sleep(delta);
        } catch (InterruptedException e) {
          logger.error("Interrupted while waiting for Revive result!");
          Thread.currentThread().interrupt();
        }
        accumulatedTime += delta;
      }
    }

    for (int i = index; i < reviveRequests.length; i++) {
      ReviveRequest request = reviveRequests[i];
      DataBatches.DataBatch batch = batches.get(i);
      if (remainReviveTimes > 0) {
        reviveFailedBatchesMap.add(batch);
      } else {
        String errorMsg =
            String.format(
                "Revive failed while pushing merged for shuffle %d map %d attempt %d partition %d batch %d location %s.",
                shuffleId, mapId, attemptId, request.partitionId, oldGroupedBatchId, batch.loc);
        pushState.exception.compareAndSet(
            null,
            new CelebornIOException(
                errorMsg,
                new CelebornIOException(
                    cause
                        + " then revive but "
                        + request.reviveStatus
                        + "("
                        + StatusCode.fromValue(request.reviveStatus)
                        + ")")));
        return;
      }
    }

    if (accumulatedTime > 0) {
      // Churny partitions (frequent split/retire) hit this per merged batch group; keep the
      // executor log readable by demoting the per-group wait summary to DEBUG.
      logger.debug(
          "Waited {} ms for revive results of {} batches for shuffle {} map {} attempt {} groupedBatch {}.",
          accumulatedTime,
          reviveRequests.length,
          shuffleId,
          mapId,
          attemptId,
          oldGroupedBatchId);
    }

    for (Map.Entry<Pair<String, String>, DataBatches> entry : newDataBatchesMap.entrySet()) {
      Pair<String, String> addressPair = entry.getKey();
      DataBatches newDataBatches = entry.getValue();
      doPushMergedData(
          addressPair,
          shuffleId,
          mapId,
          attemptId,
          newDataBatches.requireBatches(),
          pushState,
          remainReviveTimes);
    }
    if (reviveFailedBatchesMap.isEmpty()) {
      pushState.removeBatch(oldGroupedBatchId, batches.get(0).loc.hostAndPushPort());
    } else {
      ReviveRequest[] requests =
          addAndGetReviveRequests(shuffleId, mapId, attemptId, reviveFailedBatchesMap, cause);
      pushDataRetryPool.submit(
          () ->
              submitRetryPushMergedData(
                  pushState,
                  shuffleId,
                  mapId,
                  attemptId,
                  reviveFailedBatchesMap,
                  cause,
                  oldGroupedBatchId,
                  requests,
                  remainReviveTimes - 1,
                  System.currentTimeMillis()
                      + conf.clientRpcRequestPartitionLocationAskTimeout().duration().toMillis()));
    }
  }

  private Pair<String, String> genAddressPair(PartitionLocation loc) {
    if (loc.hasPeer()) {
      return Pair.of(loc.hostAndPushPort(), loc.getPeer().hostAndPushPort());
    } else {
      return Pair.of(loc.hostAndPushPort(), null);
    }
  }

  private ConcurrentHashMap<Integer, PartitionLocation> registerShuffle(
      int shuffleId, int numMappers, int numPartitions) throws CelebornIOException {
    return registerShuffleInternal(
        shuffleId,
        numMappers,
        numPartitions,
        () ->
            lifecycleManagerRef.askSync(
                new RegisterShuffle(shuffleId, numMappers, numPartitions, SerdeVersion.V1),
                conf.clientRpcRegisterShuffleAskTimeout(),
                rpcMaxRetries,
                rpcRetryWait,
                ClassTag$.MODULE$.apply(RegisterShuffleResponse.class)));
  }

  @Override
  public PartitionLocation registerMapPartitionTask(
      int shuffleId, int numMappers, int mapId, int attemptId, int partitionId) throws IOException {
    return registerMapPartitionTask(shuffleId, numMappers, mapId, attemptId, partitionId, false);
  }

  public PartitionLocation registerMapPartitionTask(
      int shuffleId,
      int numMappers,
      int mapId,
      int attemptId,
      int partitionId,
      boolean isSegmentGranularityVisible)
      throws IOException {
    logger.info(
        "Register MapPartition task for shuffle {} map {} attempt {} partition {} with {} mapper.",
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        numMappers);
    ConcurrentHashMap<Integer, PartitionLocation> partitionLocationMap =
        registerShuffleInternal(
            shuffleId,
            numMappers,
            numMappers,
            () ->
                lifecycleManagerRef.askSync(
                    RegisterMapPartitionTask$.MODULE$.apply(
                        shuffleId,
                        numMappers,
                        mapId,
                        attemptId,
                        partitionId,
                        isSegmentGranularityVisible),
                    conf.clientRpcRegisterShuffleAskTimeout(),
                    ClassTag$.MODULE$.apply(RegisterShuffleResponse.class)));

    return partitionLocationMap.get(partitionId);
  }

  @Override
  public ConcurrentHashMap<Integer, PartitionLocation> getPartitionLocation(
      int shuffleId, int numMappers, int numPartitions) throws CelebornIOException {
    ConcurrentHashMap<Integer, PartitionLocationGroup> groupMap =
        getPartitionLocationMap(shuffleId, numMappers, numPartitions);
    ConcurrentHashMap<Integer, PartitionLocation> result = JavaUtils.newConcurrentHashMap();
    groupMap.forEach(
        (partitionId, group) -> {
          PartitionLocation loc = group.latest();
          if (loc != null) {
            result.put(partitionId, loc);
          }
        });
    return result;
  }

  private ConcurrentHashMap<Integer, PartitionLocationGroup> getPartitionLocationMap(
      int shuffleId, int numMappers, int numPartitions) throws CelebornIOException {
    try {
      return reducePartitionMap.computeIfAbsent(
          shuffleId,
          (id) -> {
            try {
              ConcurrentHashMap<Integer, PartitionLocation> locations =
                  registerShuffle(shuffleId, numMappers, numPartitions);
              ConcurrentHashMap<Integer, PartitionLocationGroup> groups =
                  JavaUtils.newConcurrentHashMap();
              locations.forEach(
                  (partitionId, loc) -> groups.put(partitionId, new PartitionLocationGroup(loc)));
              return groups;
            } catch (CelebornIOException e) {
              throw new RuntimeException(e);
            }
          });
    } catch (RuntimeException e) {
      if (e.getCause() instanceof CelebornIOException) {
        throw (CelebornIOException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  @Override
  public PushState getPushState(String mapKey) {
    return pushStates.computeIfAbsent(mapKey, (s) -> new PushState(conf));
  }

  @Override
  public Tuple2<Integer, Boolean> getShuffleId(
      int appShuffleId, String appShuffleIdentifier, boolean isWriter, boolean isBarrierStage) {
    return shuffleIdCache.computeIfAbsent(
        appShuffleIdentifier,
        (id) -> {
          PbGetShuffleId pbGetShuffleId =
              PbGetShuffleId.newBuilder()
                  .setAppShuffleId(appShuffleId)
                  .setAppShuffleIdentifier(appShuffleIdentifier)
                  .setIsShuffleWriter(isWriter)
                  .setIsBarrierStage(isBarrierStage)
                  .build();
          PbGetShuffleIdResponse pbGetShuffleIdResponse =
              lifecycleManagerRef.askSync(
                  pbGetShuffleId,
                  conf.clientRpcRegisterShuffleAskTimeout(),
                  ClassTag$.MODULE$.apply(PbGetShuffleIdResponse.class));
          return Tuple2.apply(
              pbGetShuffleIdResponse.getShuffleId(), pbGetShuffleIdResponse.getSuccess());
        });
  }

  @Override
  public void readReducerPartitionEnd(
      int shuffleId,
      int partitionId,
      int startMapIndex,
      int endMapIndex,
      int crc32,
      long bytesWritten)
      throws IOException {
    PbReadReducerPartitionEnd pbReadReducerPartitionEnd =
        PbReadReducerPartitionEnd.newBuilder()
            .setShuffleId(shuffleId)
            .setPartitionId(partitionId)
            .setStartMaxIndex(startMapIndex)
            .setEndMapIndex(endMapIndex)
            .setCrc32(crc32)
            .setBytesWritten(bytesWritten)
            .build();

    PbReadReducerPartitionEndResponse pbReducerPartitionEndResponse =
        lifecycleManagerRef.askSync(
            pbReadReducerPartitionEnd,
            conf.clientRpcRegisterShuffleAskTimeout(),
            ClassTag$.MODULE$.apply(PbReadReducerPartitionEndResponse.class));
    if (pbReducerPartitionEndResponse.getStatus() != StatusCode.SUCCESS.getValue()) {
      throw new CelebornIOException(pbReducerPartitionEndResponse.getErrorMsg());
    }
  }

  @Override
  public boolean reportShuffleFetchFailure(int appShuffleId, int shuffleId, long taskId) {
    PbReportShuffleFetchFailure pbReportShuffleFetchFailure =
        PbReportShuffleFetchFailure.newBuilder()
            .setAppShuffleId(appShuffleId)
            .setShuffleId(shuffleId)
            .setTaskId(taskId)
            .build();
    PbReportShuffleFetchFailureResponse pbReportShuffleFetchFailureResponse =
        lifecycleManagerRef.askSync(
            pbReportShuffleFetchFailure,
            conf.clientRpcRegisterShuffleAskTimeout(),
            ClassTag$.MODULE$.apply(PbReportShuffleFetchFailureResponse.class));
    return pbReportShuffleFetchFailureResponse.getSuccess();
  }

  @Override
  public boolean reportBarrierTaskFailure(int appShuffleId, String appShuffleIdentifier) {
    PbReportBarrierStageAttemptFailure pbReportBarrierStageAttemptFailure =
        PbReportBarrierStageAttemptFailure.newBuilder()
            .setAppShuffleId(appShuffleId)
            .setAppShuffleIdentifier(appShuffleIdentifier)
            .build();
    PbReportBarrierStageAttemptFailureResponse pbReportBarrierStageAttemptFailureResponse =
        lifecycleManagerRef.askSync(
            pbReportBarrierStageAttemptFailure,
            conf.clientRpcRegisterShuffleAskTimeout(),
            ClassTag$.MODULE$.apply(PbReportBarrierStageAttemptFailureResponse.class));
    return pbReportBarrierStageAttemptFailureResponse.getSuccess();
  }

  private ConcurrentHashMap<Integer, PartitionLocation> registerShuffleInternal(
      int shuffleId, int numMappers, int numPartitions, Callable<RegisterShuffleResponse> callable)
      throws CelebornIOException {
    int numRetries = registerShuffleMaxRetries;
    StatusCode lastFailedStatusCode = null;
    while (numRetries > 0) {
      try {
        RegisterShuffleResponse response = callable.call();
        StatusCode respStatus = response.status();
        if (StatusCode.SUCCESS.equals(respStatus)) {
          ConcurrentHashMap<Integer, PartitionLocation> result = JavaUtils.newConcurrentHashMap();
          PartitionLocation[] locations = response.partitionLocations();
          for (PartitionLocation location : locations) {
            pushExcludedWorkers.remove(location.hostAndPushPort());
            if (location.hasPeer()) {
              pushExcludedWorkers.remove(location.getPeer().hostAndPushPort());
            }
            result.put(location.getId(), location);
          }
          return result;
        } else if (StatusCode.SLOT_NOT_AVAILABLE.equals(respStatus)) {
          lastFailedStatusCode = respStatus;
          logger.error(
              "LifecycleManager request slots return {}, retry again, remain retry times {}.",
              StatusCode.SLOT_NOT_AVAILABLE,
              numRetries - 1);
        } else if (StatusCode.RESERVE_SLOTS_FAILED.equals(respStatus)) {
          lastFailedStatusCode = respStatus;
          logger.error(
              "LifecycleManager request slots return {}, retry again, remain retry times {}.",
              StatusCode.RESERVE_SLOTS_FAILED,
              numRetries - 1);
        } else {
          lastFailedStatusCode = respStatus;
          logger.error(
              "LifecycleManager request slots return {}, retry again, remain retry times {}.",
              StatusCode.REQUEST_FAILED,
              numRetries - 1);
        }
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        logger.error(
            "Exception raised while registering shuffle {} with {} mapper and {} partitions.",
            shuffleId,
            numMappers,
            numPartitions,
            e);
        throw new CelebornIOException("Register shuffle failed for shuffle " + shuffleId + ".", e);
      }

      try {
        TimeUnit.MILLISECONDS.sleep(registerShuffleRetryWaitMs);
      } catch (InterruptedException e) {
        break;
      }
      numRetries--;
    }
    throw new CelebornIOException(
        "Register shuffle failed for shuffle " + shuffleId + ", reason: " + lastFailedStatusCode);
  }

  protected void limitMaxInFlight(String mapKey, PushState pushState, String hostAndPushPort)
      throws IOException {
    boolean reachLimit = pushState.limitMaxInFlight(hostAndPushPort);

    if (reachLimit) {
      throw new CelebornIOException(
          String.format(
              "Waiting timeout for task %s while limiting max in-flight requests to %s",
              mapKey, hostAndPushPort),
          pushState.exception.get());
    }
  }

  protected void limitZeroInFlight(String mapKey, PushState pushState) throws IOException {
    boolean reachLimit = pushState.limitZeroInFlight();

    if (reachLimit) {
      throw new CelebornIOException(
          String.format(
              "Waiting timeout for task %s while limiting zero in-flight requests", mapKey),
          pushState.exception.get());
    }
  }

  /**
   * Check if a newer PartitionLocation(with larger epoch) exists in local cache.
   *
   * @param shuffleMap The mapping between shuffle id and partition location group.
   * @param partitionId The id of partition.
   * @param epoch The epoch of revive.
   * @param wait Whether to wait for some time for a newer partition location.
   * @return whether newer partition location exists in local cache.
   */
  boolean newerPartitionLocationExists(
      Map<Integer, PartitionLocationGroup> shuffleMap, int partitionId, int epoch, boolean wait) {
    PartitionLocationGroup currentGroup = shuffleMap == null ? null : shuffleMap.get(partitionId);
    if (currentGroup != null && currentGroup.maxEpoch() > epoch) {
      return true;
    } else if (wait) {
      long sleepTimeMs = RND.nextInt(50);
      if (sleepTimeMs > 30) {
        try {
          TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          logger.error("Waiting revived location was interrupted.", e);
          Thread.currentThread().interrupt();
        }
      }

      currentGroup = shuffleMap == null ? null : shuffleMap.get(partitionId);
      return currentGroup != null && currentGroup.maxEpoch() > epoch;
    } else {
      return false;
    }
  }

  void excludeWorkerByCause(StatusCode cause, PartitionLocation oldLocation) {
    if (pushExcludeWorkerOnFailureEnabled && oldLocation != null) {
      switch (cause) {
        case PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY:
        case PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY:
        case PUSH_DATA_TIMEOUT_PRIMARY:
          pushExcludedWorkers.add(oldLocation.hostAndPushPort());
          break;
        case PUSH_DATA_CREATE_CONNECTION_FAIL_REPLICA:
        case PUSH_DATA_CONNECTION_EXCEPTION_REPLICA:
        case PUSH_DATA_TIMEOUT_REPLICA:
          pushExcludedWorkers.add(oldLocation.getPeer().hostAndPushPort());
          break;
        default: // fall out
      }
    }
  }

  /**
   * Blocking revive for the all-locations-unusable case of adaptive parallelism. Besides the fresh
   * location request for the partition's max epoch, every locally retired location still held in
   * the active list is forwarded as a retire report — the LM counts those epochs as surviving until
   * the report arrives, so without them the gap-based allocation finds gap == 0, re-replies the
   * same retired epochs, and this executor is left with no writable location (the NULL-location
   * failure). Returns the response status per partition, or null on RPC failure.
   */
  private Map<Integer, Integer> reviveWithRetires(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      int epoch,
      PartitionLocation oldLocation,
      StatusCode cause,
      PartitionLocationGroup group) {
    excludeWorkerByCause(cause, oldLocation);

    Set<Integer> mapIds = new HashSet<>();
    mapIds.add(mapId);
    List<ReviveRequest> requests = new ArrayList<>();
    requests.add(
        new ReviveRequest(shuffleId, mapId, attemptId, partitionId, epoch, oldLocation, cause));
    for (PartitionLocationGroup.OutstandingRetire retire : group.outstandingRetires()) {
      // Skip the primary request's epoch — it is already carried by the request above.
      if (retire.location.getEpoch() != epoch) {
        requests.add(
            new ReviveRequest(
                shuffleId,
                mapId,
                attemptId,
                partitionId,
                retire.location.getEpoch(),
                retire.location,
                retire.cause));
      }
    }
    return reviveBatch(shuffleId, mapIds, requests);
  }

  /**
   * Revive with all outstanding retire reports attached, up to {@code maxAttempts} times, until the
   * partition has a writable location for {@code mapId}. Each attempt re-snapshots the group: a
   * previous attempt may have delivered locations that a concurrent retire took down again, so the
   * retire reports of the next attempt reflect the newest local state. Returns the usable location,
   * or null when attempts are exhausted / the RPC fails / the mapper has ended (the push caller
   * treats a null return as unusable and skips or fails accordingly).
   */
  private PartitionLocation reviveUntilUsable(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      PartitionLocationGroup group,
      int maxAttempts) {
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      if (mapperEnded(shuffleId, mapId)) {
        return null;
      }
      long reviveStartMs = System.currentTimeMillis();
      Map<Integer, Integer> results =
          reviveWithRetires(
              shuffleId,
              mapId,
              attemptId,
              partitionId,
              group.maxEpoch(),
              group.latest(),
              StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_PRIMARY,
              group);
      long elapsedMs = System.currentTimeMillis() - reviveStartMs;
      boolean success =
          results != null
              && results.containsKey(partitionId)
              && results.get(partitionId) == StatusCode.SUCCESS.getValue();
      logger.info(
          "Blocking revive for shuffle {} map {} attempt {} partition {} epoch {} attempt {}/{} {} after {} ms.",
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          group.maxEpoch(),
          attempt,
          maxAttempts,
          success ? "succeeded" : "failed",
          elapsedMs);
      if (success) {
        PartitionLocation loc = group.currentFor(mapId);
        if (loc != null) {
          return loc;
        }
        // SUCCESS but nothing writable: the response carried only epochs this executor has
        // already retired (stale active set on the LM side). Fall through and retry — the
        // next attempt forwards the retire reports of those very epochs.
        logger.warn(
            "Shuffle {} partition {}: blocking revive returned SUCCESS but no writable location "
                + "(attempt {}/{}); retrying with outstanding retire reports.",
            shuffleId,
            partitionId,
            attempt,
            maxAttempts);
      }
    }
    return null;
  }

  private boolean revive(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      int epoch,
      PartitionLocation oldLocation,
      StatusCode cause) {
    excludeWorkerByCause(cause, oldLocation);

    Set<Integer> mapIds = new HashSet<>();
    mapIds.add(mapId);
    List<ReviveRequest> requests = new ArrayList<>();
    ReviveRequest req =
        new ReviveRequest(shuffleId, mapId, attemptId, partitionId, epoch, oldLocation, cause);
    requests.add(req);
    long reviveStartMs = System.currentTimeMillis();
    Map<Integer, Integer> results = reviveBatch(shuffleId, mapIds, requests);
    long reviveElapsedMs = System.currentTimeMillis() - reviveStartMs;

    if (mapperEnded(shuffleId, mapId)) {
      logger.debug(
          "Revive success, but the mapper ended for shuffle {} map {} attempt {} partition {}, just return true(Assume revive successfully).",
          shuffleId,
          mapId,
          attemptId,
          partitionId);
      return true;
    } else {
      boolean success =
          results != null
              && results.containsKey(partitionId)
              && results.get(partitionId) == StatusCode.SUCCESS.getValue();
      // Map tasks block here waiting for the new location; record how long the wait took.
      logger.info(
          "Blocking revive for shuffle {} map {} attempt {} partition {} epoch {} (cause {}) {} after {} ms.",
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          epoch,
          cause,
          success ? "succeeded" : "failed",
          reviveElapsedMs);
      return success;
    }
  }

  /** @return partitionId -> StatusCode#getValue */
  Map<Integer, Integer> reviveBatch(
      int shuffleId, Set<Integer> mapIds, Collection<ReviveRequest> requests) {
    // partitionId -> StatusCode#getValue
    Map<Integer, Integer> results = new HashMap<>();

    // Local cached map of (partitionId -> PartitionLocationGroup)
    ConcurrentHashMap<Integer, PartitionLocationGroup> partitionLocationMap =
        reducePartitionMap.get(shuffleId);

    Map<Integer, PartitionLocation> oldLocMap = new HashMap<>();
    Iterator<ReviveRequest> iter = requests.iterator();
    while (iter.hasNext()) {
      ReviveRequest req = iter.next();
      oldLocMap.put(req.partitionId, req.loc);
    }
    try {
      ChangeLocationResponse response =
          lifecycleManagerRef.askSync(
              Revive$.MODULE$.apply(
                  shuffleId, new ArrayList<>(mapIds), new ArrayList<>(requests), SerdeVersion.V1),
              conf.clientRpcRequestPartitionLocationAskTimeout(),
              ClassTag$.MODULE$.apply(ChangeLocationResponse.class));

      for (int i = 0; i < response.endedMapIds().size(); i++) {
        int mapId = response.endedMapIds().get(i);
        mapperEndMap.computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet()).add(mapId);
      }

      for (Map.Entry<Integer, Tuple3<StatusCode, Boolean, PartitionLocation>> entry :
          response.newLocs().entrySet()) {
        int partitionId = entry.getKey();
        StatusCode statusCode = entry.getValue()._1();
        if (entry.getValue()._2() != null) {
          PartitionLocation oldLoc = oldLocMap.get(partitionId);
          if (oldLoc != null) {
            // Currently, revive only check if main location available, here won't remove peer loc.
            pushExcludedWorkers.remove(oldLoc.hostAndPushPort());
          }
        }

        if (StatusCode.SUCCESS == statusCode) {
          PartitionLocation loc = entry.getValue()._3();
          PartitionLocationGroup group =
              partitionLocationMap.computeIfAbsent(
                  partitionId, id -> new PartitionLocationGroup(loc));
          if (adaptivePartitionWriteParallelismEnabled) {
            // Converge to the full active set delivered by the LifecycleManager.
            List<PartitionLocation> allActive = new ArrayList<>();
            if (loc != null) {
              allActive.add(loc);
            }
            List<PartitionLocation> additionals = response.additionalLocs().get(partitionId);
            if (additionals != null) {
              allActive.addAll(additionals);
            }
            int beforeActive = group.activeCount();
            int beforeRetired = group.retiredCount();
            group.mergeActiveLocations(allActive, true);
            int afterActive = group.activeCount();
            if (afterActive > beforeActive && afterActive > 1) {
              StringBuilder sb = new StringBuilder();
              for (PartitionLocation l : allActive) {
                if (sb.length() > 0) {
                  sb.append(",");
                }
                sb.append(l.getEpoch()).append("@").append(l.hostAndPushPort());
              }
              logger.info(
                  "Shuffle {} partition {}: parallel write active, now writing to {} locations: {}.",
                  shuffleId,
                  partitionId,
                  afterActive,
                  sb);
            }
            int evicted = beforeRetired - group.retiredCount();
            if (evicted > 0) {
              logger.info(
                  "Shuffle {} partition {}: evicted {} retired location(s) no longer reported "
                      + "by LifecycleManager.",
                  shuffleId,
                  partitionId,
                  evicted);
            }
          } else if (loc != null) {
            group.updateLatest(loc);
          }
          if (loc != null) {
            pushExcludedWorkers.remove(loc.hostAndPushPort());
            if (loc.hasPeer()) {
              pushExcludedWorkers.remove(loc.getPeer().hostAndPushPort());
            }
          }
        } else if (StatusCode.STAGE_ENDED == statusCode) {
          stageEndShuffleSet.add(shuffleId);
          return results;
        } else if (StatusCode.SHUFFLE_UNREGISTERED == statusCode) {
          logger.error("SHUFFLE_NOT_REGISTERED!");
          return null;
        }
        results.put(partitionId, (int) (statusCode.getValue()));
      }

      return results;
    } catch (Exception e) {
      StringBuilder partitionIds = new StringBuilder();
      StringBuilder epochs = new StringBuilder();
      requests.forEach(
          (req) -> {
            partitionIds.append(req.partitionId).append(",");
            epochs.append(req.epoch).append(",");
          });
      logger.error(
          "Exception raised while reviving for shuffle {} partitionIds {} epochs {}.",
          shuffleId,
          partitionIds,
          epochs,
          e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return null;
    }
  }

  private interface PushDataRpcResponseCallback extends RpcResponseCallback {
    default void updateLatestPartition(PartitionLocation latest) {}
  }

  public int pushOrMergeData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions,
      boolean doPush,
      boolean skipCompress)
      throws IOException {
    // mapKey
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    // return if shuffle stage already ended
    if (mapperEnded(shuffleId, mapId)) {
      logger.debug(
          "Push or merge data ignored because mapper already ended for shuffle {} map {} attempt {} partition {}.",
          shuffleId,
          mapId,
          attemptId,
          partitionId);
      PushState pushState = pushStates.get(mapKey);
      if (pushState != null) {
        pushState.cleanup();
      }
      return 0;
    }
    // register shuffle if not registered
    final ConcurrentHashMap<Integer, PartitionLocationGroup> map =
        getPartitionLocationMap(shuffleId, numMappers, numPartitions);

    // get location
    // If rerun or speculation task running after LifecycleManager call stageEnd,
    // register shuffle will return an empty location map, client need revive for a new location.
    if (!map.containsKey(partitionId)) {
      if (!revive(
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          -1,
          null,
          StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_PRIMARY)) {
        throw new CelebornIOException(
            String.format("Revive for shuffle %s partition %d failed.", shuffleId, partitionId));
      }
    }

    if (mapperEnded(shuffleId, mapId)) {
      logger.debug(
          "Push or merge data ignored because mapper already ended for shuffle {} map {} attempt {} partition {}.",
          shuffleId,
          mapId,
          attemptId,
          partitionId);
      PushState pushState = pushStates.get(mapKey);
      if (pushState != null) {
        pushState.cleanup();
      }
      return 0;
    }

    PartitionLocationGroup group = map.get(partitionId);
    PartitionLocation currentLoc = group == null ? null : group.currentFor(mapId);
    if (currentLoc == null && group != null && adaptivePartitionWriteParallelismEnabled) {
      // All known locations of this partition are unusable. Revive with every outstanding
      // retire report attached, then retry a bounded number of times: a single response may
      // still carry only epochs this executor has already retired (the LM digests retire
      // reports with a delay), so one revive is not guaranteed to yield a writable location.
      // Retrying beats failing the task with a NULL location on a transiently stale active set.
      logger.info(
          "Shuffle {} partition {}: all {} location(s) unusable, blocking revive from epoch {}.",
          shuffleId,
          partitionId,
          group.activeCount(),
          group.maxEpoch());
      currentLoc =
          reviveUntilUsable(shuffleId, mapId, attemptId, partitionId, group, /* maxAttempts= */ 3);
      if (currentLoc != null) {
        logger.info(
            "Shuffle {} partition {}: blocking revive succeeded, writing to epoch {}@{}.",
            shuffleId,
            partitionId,
            currentLoc.getEpoch(),
            currentLoc.hostAndPushPort());
      }
    }
    final PartitionLocation loc = currentLoc;
    if (loc == null) {
      throw new CelebornIOException(
          String.format(
              "Partition location for shuffle %s partition %d is NULL! Active epochs: %s; retired: %s.",
              shuffleId,
              partitionId,
              group == null ? "[]" : group.activeEpochsSnapshot(),
              group == null ? "[]" : group.retiredEpochsSnapshot()));
    }

    PushState pushState = getPushState(mapKey);

    // increment batchId
    final int nextBatchId = pushState.nextBatchId();

    if (shuffleCompressionEnabled && !skipCompress) {
      // compress data
      final Compressor compressor = compressorThreadLocal.get();
      compressor.compress(data, offset, length);

      data = compressor.getCompressedBuffer();
      offset = 0;
      length = compressor.getCompressedTotalSize();
    }

    // Snapshot volatile field once to avoid a TOCTOU race between isPresent() and get().
    Optional<CryptoHandler> handler = cryptoHandler;
    if (handler.isPresent()) {
      byte[] encrypted = handler.get().encrypt(data, offset, length);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Encrypted shuffle data for shuffle {} map {} partition {}: {} bytes -> {} bytes.",
            shuffleId,
            mapId,
            partitionId,
            length,
            encrypted.length);
      }
      data = encrypted;
      offset = 0;
      length = encrypted.length;
    }

    final byte[] body = new byte[BATCH_HEADER_SIZE + length];
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET, mapId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 4, attemptId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 8, nextBatchId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 12, length);
    System.arraycopy(data, offset, body, BATCH_HEADER_SIZE, length);

    if (doPush) {
      // check limit
      limitMaxInFlight(mapKey, pushState, loc.hostAndPushPort());

      // add inFlight requests
      pushState.addBatch(nextBatchId, body.length, loc.hostAndPushPort());
      final long pushStartTime = System.nanoTime();

      // build PushData request
      NettyManagedBuffer buffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(body));
      final String shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId);
      PushData pushData = new PushData(PRIMARY_MODE, shuffleKey, loc.getUniqueId(), buffer);

      // build callback
      RpcResponseCallback callback =
          new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
              if (response.remaining() > 0 && response.get() == StatusCode.MAP_ENDED.getValue()) {
                mapperEndMap
                    .computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet())
                    .add(mapId);
              }
              logger.debug(
                  "Push data to {} success for shuffle {} map {} attempt {} partition {} batch {}.",
                  loc.hostAndPushPort(),
                  shuffleId,
                  mapId,
                  attemptId,
                  partitionId,
                  nextBatchId);
            }

            @Override
            public void onFailure(Throwable e) {
              String errorMsg =
                  String.format(
                      "Push data to %s failed for shuffle %d map %d attempt %d partition %d batch %d.",
                      loc, shuffleId, mapId, attemptId, partitionId, nextBatchId);
              pushState.exception.compareAndSet(null, new CelebornIOException(errorMsg, e));
            }
          };

      RpcResponseCallback wrappedCallback =
          new PushDataRpcResponseCallback() {
            int remainReviveTimes = maxReviveTimes;
            PartitionLocation latest = loc;

            @Override
            public void updateLatestPartition(PartitionLocation newloc) {
              pushState.addBatch(nextBatchId, body.length, newloc.hostAndPushPort());
              pushState.removeBatch(nextBatchId, this.latest.hostAndPushPort());
              this.latest = newloc;
            }

            @Override
            public void onSuccess(ByteBuffer response) {
              long pushRttNanos = System.nanoTime() - pushStartTime;
              pushState.recordPushRtt(pushRttNanos, slowPushThresholdNanos);
              if (pushRttNanos > slowPushThresholdNanos) {
                logger.warn(
                    "Slow push data to {} for shuffle {} map {} attempt {} partition {} batch {}, rtt {} ms.",
                    latest.hostAndPushPort(),
                    shuffleId,
                    mapId,
                    attemptId,
                    partitionId,
                    nextBatchId,
                    TimeUnit.NANOSECONDS.toMillis(pushRttNanos));
              }
              if (response.remaining() > 0) {
                byte reason = response.get();
                if (reason == StatusCode.SOFT_SPLIT.getValue()) {
                  logger.debug(
                      "Push data to {} soft split required for shuffle {} map {} attempt {} partition {} batch {}.",
                      latest.hostAndPushPort(),
                      shuffleId,
                      mapId,
                      attemptId,
                      partitionId,
                      nextBatchId);
                  handleSoftSplitRetire(shuffleId, mapId, attemptId, partitionId, latest);
                  recordPushEvent(
                      shuffleId, mapId, attemptId, latest.hostAndPushPort(), PushEvent.SOFT_SPLIT);
                  pushState.onSuccess(latest.hostAndPushPort());
                  pushState.removeBatch(nextBatchId, latest.hostAndPushPort());
                  callback.onSuccess(response);
                } else if (reason == StatusCode.HARD_SPLIT.getValue()) {
                  logger.debug(
                      "Push data to {} hard split required for shuffle {} map {} attempt {} partition {} batch {}.",
                      latest.hostAndPushPort(),
                      shuffleId,
                      mapId,
                      attemptId,
                      partitionId,
                      nextBatchId);
                  if (dataPushFailureTrackingEnabled && pushReplicateEnabled) {
                    pushState.recordFailedBatch(
                        latest.getUniqueId(), mapId, attemptId, nextBatchId);
                  }
                  ReviveRequest reviveRequest =
                      new ReviveRequest(
                          shuffleId,
                          mapId,
                          attemptId,
                          partitionId,
                          latest.getEpoch(),
                          latest,
                          StatusCode.HARD_SPLIT);
                  reviveManager.addRequest(reviveRequest);
                  recordPushEvent(
                      shuffleId, mapId, attemptId, latest.hostAndPushPort(), PushEvent.HARD_SPLIT);
                  if (adaptivePartitionWriteParallelismEnabled) {
                    PartitionLocationGroup latestGroup = locationGroup(shuffleId, partitionId);
                    if (latestGroup != null) {
                      retireAndPresetIfAnotherUsable(
                          shuffleId,
                          mapId,
                          latestGroup,
                          latest.getEpoch(),
                          StatusCode.HARD_SPLIT,
                          reviveRequest);
                    }
                  }
                  long dueTime =
                      System.currentTimeMillis()
                          + conf.clientRpcRequestPartitionLocationAskTimeout()
                              .duration()
                              .toMillis();
                  pushDataRetryPool.submit(
                      () ->
                          submitRetryPushData(
                              shuffleId,
                              body,
                              nextBatchId,
                              this,
                              pushState,
                              reviveRequest,
                              remainReviveTimes,
                              dueTime));
                } else if (reason == StatusCode.PUSH_DATA_SUCCESS_PRIMARY_CONGESTED.getValue()) {
                  logger.debug(
                      "Push data to {} primary congestion required for shuffle {} map {} attempt {} partition {} batch {}.",
                      latest.hostAndPushPort(),
                      shuffleId,
                      mapId,
                      attemptId,
                      partitionId,
                      nextBatchId);
                  pushState.onCongestControl(latest.hostAndPushPort());
                  recordPushEvent(
                      shuffleId,
                      mapId,
                      attemptId,
                      latest.hostAndPushPort(),
                      PushEvent.PRIMARY_CONGESTED);
                  pushState.removeBatch(nextBatchId, latest.hostAndPushPort());
                  callback.onSuccess(response);
                } else if (reason == StatusCode.PUSH_DATA_SUCCESS_REPLICA_CONGESTED.getValue()) {
                  logger.debug(
                      "Push data to {} replica congestion required for shuffle {} map {} attempt {} partition {} batch {}.",
                      latest.hostAndPushPort(),
                      shuffleId,
                      mapId,
                      attemptId,
                      partitionId,
                      nextBatchId);
                  pushState.onCongestControl(latest.hostAndPushPort());
                  recordPushEvent(
                      shuffleId,
                      mapId,
                      attemptId,
                      latest.hostAndPushPort(),
                      PushEvent.REPLICA_CONGESTED);
                  pushState.removeBatch(nextBatchId, latest.hostAndPushPort());
                  callback.onSuccess(response);
                } else {
                  // StageEnd.
                  response.rewind();
                  pushState.onSuccess(latest.hostAndPushPort());
                  pushState.removeBatch(nextBatchId, latest.hostAndPushPort());
                  callback.onSuccess(response);
                }
              } else {
                pushState.onSuccess(latest.hostAndPushPort());
                pushState.removeBatch(nextBatchId, latest.hostAndPushPort());
                callback.onSuccess(response);
              }
            }

            @Override
            public void onFailure(Throwable e) {
              if (dataPushFailureTrackingEnabled) {
                pushState.recordFailedBatch(latest.getUniqueId(), mapId, attemptId, nextBatchId);
              }
              if (pushState.exception.get() != null) {
                return;
              }
              if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                callback.onFailure(e);
                return;
              }
              StatusCode cause = getPushDataFailCause(e.getMessage());
              if (remainReviveTimes <= 0) {
                if (e instanceof CelebornIOException) {
                  callback.onFailure(e);
                } else {
                  callback.onFailure(new CelebornIOException(cause, e));
                }
                return;
              }

              logger.error(
                  "Push data to {} failed for shuffle {} map {} attempt {} partition {} batch {}, remain revive times {}.",
                  latest.hostAndPushPort(),
                  shuffleId,
                  mapId,
                  attemptId,
                  partitionId,
                  nextBatchId,
                  remainReviveTimes,
                  e);
              // async retry push data
              if (!mapperEnded(shuffleId, mapId)) {
                remainReviveTimes = remainReviveTimes - 1;
                ReviveRequest reviveRequest =
                    new ReviveRequest(
                        shuffleId, mapId, attemptId, partitionId, latest.getEpoch(), latest, cause);
                reviveManager.addRequest(reviveRequest);
                if (adaptivePartitionWriteParallelismEnabled) {
                  PartitionLocationGroup latestGroup = locationGroup(shuffleId, partitionId);
                  if (latestGroup != null) {
                    retireAndPresetIfAnotherUsable(
                        shuffleId, mapId, latestGroup, latest.getEpoch(), cause, reviveRequest);
                  }
                }
                long dueTime =
                    System.currentTimeMillis()
                        + conf.clientRpcRequestPartitionLocationAskTimeout().duration().toMillis();
                pushDataRetryPool.submit(
                    () ->
                        submitRetryPushData(
                            shuffleId,
                            body,
                            nextBatchId,
                            this,
                            pushState,
                            reviveRequest,
                            remainReviveTimes,
                            dueTime));
              } else {
                pushState.removeBatch(nextBatchId, latest.hostAndPushPort());
                logger.info(
                    "Push data to {} failed but mapper already ended for shuffle {} map {} attempt {} partition {} batch {}, remain revive times {}.",
                    latest.hostAndPushPort(),
                    shuffleId,
                    mapId,
                    attemptId,
                    partitionId,
                    nextBatchId,
                    remainReviveTimes);
              }
            }
          };

      // do push data
      try {
        if (!isPushTargetWorkerExcluded(loc, wrappedCallback)) {
          if (!testRetryRevive) {
            assert dataClientFactory != null;
            TransportClient client =
                dataClientFactory.createClient(loc.getHost(), loc.getPushPort(), partitionId);
            client.pushData(pushData, pushDataTimeout, wrappedCallback);
          } else {
            wrappedCallback.onFailure(
                new CelebornIOException(
                    StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_PRIMARY,
                    new RuntimeException("Mock push data first time failed.")));
          }
        }
      } catch (Exception e) {
        logger.error(
            "Exception raised while pushing data for shuffle {} map {} attempt {} partition {} batch {} location {}.",
            shuffleId,
            mapId,
            attemptId,
            partitionId,
            nextBatchId,
            loc,
            e);
        if (e instanceof InterruptedException) {
          wrappedCallback.onFailure(e);
        } else {
          wrappedCallback.onFailure(
              new CelebornIOException(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY, e));
        }
      }
    } else {
      // add batch data
      logger.debug("Merge batch {}.", nextBatchId);
      Pair<String, String> addressPair = genAddressPair(loc);
      boolean shouldPush = pushState.addBatchData(addressPair, loc, nextBatchId, body);
      if (shouldPush) {
        limitMaxInFlight(mapKey, pushState, loc.hostAndPushPort());
        DataBatches dataBatches = pushState.takeDataBatches(addressPair);
        if (dataBatches != null) {
          doPushMergedData(
              addressPair,
              shuffleId,
              mapId,
              attemptId,
              dataBatches.requireBatches(),
              pushState,
              maxReviveTimes);
        }
      }
    }

    return body.length;
  }

  @Override
  public int pushData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException {
    return pushOrMergeData(
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        data,
        offset,
        length,
        numMappers,
        numPartitions,
        true,
        false);
  }

  @Override
  public int mergeData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException {
    return pushOrMergeData(
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        data,
        offset,
        length,
        numMappers,
        numPartitions,
        false,
        false);
  }

  @Override
  public void computeBatchCRC(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length) {
    if (!shuffleIntegrityCheckEnabled) {
      return;
    }
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    if (mapperEnded(shuffleId, mapId)) {
      return;
    }
    PushState pushState = getPushState(mapKey);
    pushState.addDataWithOffsetAndLength(partitionId, data, offset, length);
  }

  @Override
  public void pushMergedData(int shuffleId, int mapId, int attemptId) throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = pushStates.get(mapKey);
    if (pushState == null) {
      return;
    }
    ArrayList<Map.Entry<Pair<String, String>, DataBatches>> batchesArr =
        new ArrayList<>(pushState.batchesMap.entrySet());
    while (!batchesArr.isEmpty()) {
      Map.Entry<Pair<String, String>, DataBatches> entry =
          batchesArr.get(RND.nextInt(batchesArr.size()));
      limitMaxInFlight(mapKey, pushState, entry.getKey().getLeft());
      ArrayList<DataBatches.DataBatch> batches = entry.getValue().requireBatches(pushBufferMaxSize);
      if (entry.getValue().getTotalSize() == 0) {
        batchesArr.remove(entry);
      }
      doPushMergedData(
          entry.getKey(), shuffleId, mapId, attemptId, batches, pushState, maxReviveTimes);
    }
  }

  private void doPushMergedData(
      Pair<String, String> addressPair,
      int shuffleId,
      int mapId,
      int attemptId,
      ArrayList<DataBatches.DataBatch> batches,
      PushState pushState,
      int remainReviveTimes) {
    String hostPort = addressPair.getLeft();
    String[] hostPortArr = Utils.parseColonSeparatedHostPorts(hostPort, 1);
    final String host = hostPortArr[0];
    final int port = Integer.parseInt(hostPortArr[1]);
    final long pushStartTime = System.nanoTime();

    int groupedBatchId = pushState.nextBatchId();
    int groupedBatchBytesSize = batches.stream().mapToInt(batch -> batch.body.length).sum();
    pushState.addBatch(groupedBatchId, groupedBatchBytesSize, hostPort);

    final int numBatches = batches.size();
    final Integer[] partitionIds = new Integer[numBatches];
    final String[] partitionUniqueIds = new String[numBatches];
    final int[] offsets = new int[numBatches];
    final int[] batchIds = new int[numBatches];
    int currentSize = 0;
    CompositeByteBuf byteBuf = Unpooled.compositeBuffer();
    for (int i = 0; i < numBatches; i++) {
      DataBatches.DataBatch batch = batches.get(i);
      partitionIds[i] = batch.loc.getId();
      partitionUniqueIds[i] = batch.loc.getUniqueId();
      offsets[i] = currentSize;
      batchIds[i] = batch.batchId;
      currentSize += batch.body.length;
      byteBuf.addComponent(true, Unpooled.wrappedBuffer(batch.body));
    }
    NettyManagedBuffer buffer = new NettyManagedBuffer(byteBuf);
    String shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId);
    PushMergedData mergedData =
        new PushMergedData(PRIMARY_MODE, shuffleKey, partitionUniqueIds, offsets, buffer);

    RpcResponseCallback callback =
        new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer response) {
            logger.debug(
                "Push merged data to {} success for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}.",
                addressPair,
                shuffleId,
                mapId,
                attemptId,
                Arrays.toString(partitionIds),
                groupedBatchId,
                Arrays.toString(batchIds));
            pushState.removeBatch(groupedBatchId, hostPort);
            if (response.remaining() > 0 && response.get() == StatusCode.MAP_ENDED.getValue()) {
              mapperEndMap
                  .computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet())
                  .add(mapId);
            }
          }

          @Override
          public void onFailure(Throwable e) {
            String errorMsg =
                String.format(
                    "Push merged data to %s failed for shuffle %d map %d attempt %d partition %s groupedBatch %d batch %s, remain revive times %d.",
                    addressPair,
                    shuffleId,
                    mapId,
                    attemptId,
                    Arrays.toString(partitionIds),
                    groupedBatchId,
                    Arrays.toString(batchIds),
                    remainReviveTimes);
            pushState.exception.compareAndSet(null, new CelebornIOException(errorMsg, e));
            if (logger.isDebugEnabled()) {
              for (int i = 0; i < numBatches; i++) {
                logger.debug(
                    "Push merged data to {} failed for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}, remain revive times {}.",
                    addressPair,
                    shuffleId,
                    mapId,
                    attemptId,
                    partitionIds[i],
                    groupedBatchId,
                    batchIds[i],
                    remainReviveTimes);
              }
            }
          }
        };

    RpcResponseCallback wrappedCallback =
        new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer response) {
            long pushRttNanos = System.nanoTime() - pushStartTime;
            pushState.recordPushRtt(pushRttNanos, slowPushThresholdNanos);
            if (pushRttNanos > slowPushThresholdNanos) {
              logger.warn(
                  "Slow push merged data to {} for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}, rtt {} ms.",
                  addressPair,
                  shuffleId,
                  mapId,
                  attemptId,
                  Arrays.toString(partitionIds),
                  groupedBatchId,
                  Arrays.toString(batchIds),
                  TimeUnit.NANOSECONDS.toMillis(pushRttNanos));
            }
            byte reason = response.get();
            if (reason == StatusCode.HARD_SPLIT.getValue()
                || reason == StatusCode.SOFT_SPLIT.getValue()) {
              ArrayList<DataBatches.DataBatch> batchesNeedResubmit;
              if (response.remaining() > 0) {
                batchesNeedResubmit = new ArrayList<>();
                PbPushMergedDataSplitPartitionInfo partitionInfo;
                try {
                  partitionInfo = TransportMessage.fromByteBuffer(response).getParsedPayload();
                } catch (CelebornIOException | InvalidProtocolBufferException e) {
                  callback.onFailure(
                      new CelebornIOException("parse pushMergedData response failed", e));
                  return;
                }
                List<Integer> splitPartitionIndexes = partitionInfo.getSplitPartitionIndexesList();
                List<Integer> statusCodeList = partitionInfo.getStatusCodesList();
                StringBuilder dataBatchReviveInfos = new StringBuilder();
                for (int i = 0; i < splitPartitionIndexes.size(); i++) {
                  int partitionIndex = splitPartitionIndexes.get(i);
                  int batchId = batches.get(partitionIndex).batchId;
                  dataBatchReviveInfos.append(
                      String.format(
                          "(batchId=%d, partitionId=%d, cause=%s)",
                          batchId,
                          partitionIds[partitionIndex],
                          StatusCode.fromValue(statusCodeList.get(i).byteValue())));
                  if (statusCodeList.get(i) == StatusCode.SOFT_SPLIT.getValue()) {
                    PartitionLocation loc = batches.get(partitionIndex).loc;
                    handleSoftSplitRetire(shuffleId, mapId, attemptId, loc.getId(), loc);
                    recordPushEvent(
                        shuffleId, mapId, attemptId, loc.hostAndPushPort(), PushEvent.SOFT_SPLIT);
                  } else {
                    batchesNeedResubmit.add(batches.get(partitionIndex));
                  }
                }
                logger.info(
                    "Push merged data to {} partial success required for shuffle {} map {} attempt {} groupedBatch {}. split batches {}.",
                    addressPair,
                    shuffleId,
                    mapId,
                    attemptId,
                    groupedBatchId,
                    dataBatchReviveInfos);
              } else {
                // Workers that do not incorporate changes from [CELEBORN-1721]
                // will respond with a status of HARD_SPLIT,
                // but will not include a PbPushMergedDataSplitPartitionInfo.
                // For backward compatibility, all batches must be resubmitted.
                batchesNeedResubmit = batches;
                logger.info(
                    "Push merged data to {} hard split required for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}.",
                    addressPair,
                    shuffleId,
                    mapId,
                    attemptId,
                    Arrays.toString(partitionIds),
                    groupedBatchId,
                    Arrays.toString(batchIds));
              }
              if (batchesNeedResubmit.isEmpty()) {
                pushState.onSuccess(hostPort);
                callback.onSuccess(ByteBuffer.wrap(new byte[] {StatusCode.SOFT_SPLIT.getValue()}));
              } else {
                for (DataBatches.DataBatch resubmitBatch : batchesNeedResubmit) {
                  recordPushEvent(
                      shuffleId,
                      mapId,
                      attemptId,
                      resubmitBatch.loc.hostAndPushPort(),
                      PushEvent.HARD_SPLIT);
                }
                if (dataPushFailureTrackingEnabled && pushReplicateEnabled) {
                  for (DataBatches.DataBatch resubmitBatch : batchesNeedResubmit) {
                    pushState.recordFailedBatch(
                        resubmitBatch.loc.getUniqueId(), mapId, attemptId, resubmitBatch.batchId);
                  }
                }
                ReviveRequest[] requests =
                    addAndGetReviveRequests(
                        shuffleId, mapId, attemptId, batchesNeedResubmit, StatusCode.HARD_SPLIT);
                presetSuccessIfAnotherUsable(shuffleId, mapId, requests, StatusCode.HARD_SPLIT);
                pushDataRetryPool.submit(
                    () ->
                        submitRetryPushMergedData(
                            pushState,
                            shuffleId,
                            mapId,
                            attemptId,
                            batchesNeedResubmit,
                            StatusCode.HARD_SPLIT,
                            groupedBatchId,
                            requests,
                            remainReviveTimes,
                            System.currentTimeMillis()
                                + conf.clientRpcRequestPartitionLocationAskTimeout()
                                    .duration()
                                    .toMillis()));
              }
            } else if (reason == StatusCode.PUSH_DATA_SUCCESS_PRIMARY_CONGESTED.getValue()) {
              logger.debug(
                  "Push merged data to {} primary congestion required for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}.",
                  addressPair,
                  shuffleId,
                  mapId,
                  attemptId,
                  Arrays.toString(partitionIds),
                  groupedBatchId,
                  Arrays.toString(batchIds));
              pushState.onCongestControl(hostPort);
              recordPushEvent(shuffleId, mapId, attemptId, hostPort, PushEvent.PRIMARY_CONGESTED);
              callback.onSuccess(response);
            } else if (reason == StatusCode.PUSH_DATA_SUCCESS_REPLICA_CONGESTED.getValue()) {
              logger.debug(
                  "Push merged data to {} replica congestion required for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}.",
                  addressPair,
                  shuffleId,
                  mapId,
                  attemptId,
                  Arrays.toString(partitionIds),
                  groupedBatchId,
                  Arrays.toString(batchIds));
              pushState.onCongestControl(hostPort);
              recordPushEvent(shuffleId, mapId, attemptId, hostPort, PushEvent.REPLICA_CONGESTED);
              callback.onSuccess(response);
            } else if (reason == StatusCode.MAP_ENDED.getValue()) {
              pushState.onSuccess(hostPort);
              callback.onSuccess(ByteBuffer.wrap(new byte[] {StatusCode.MAP_ENDED.getValue()}));
            } else { // success
              pushState.onSuccess(hostPort);
              callback.onSuccess(ByteBuffer.wrap(new byte[] {StatusCode.SUCCESS.getValue()}));
            }
          }

          @Override
          public void onFailure(Throwable e) {
            if (dataPushFailureTrackingEnabled) {
              for (int i = 0; i < numBatches; i++) {
                pushState.recordFailedBatch(partitionUniqueIds[i], mapId, attemptId, batchIds[i]);
              }
            }
            if (pushState.exception.get() != null) {
              return;
            }
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
              callback.onFailure(e);
              return;
            }
            StatusCode cause = getPushDataFailCause(e.getMessage());
            if (remainReviveTimes <= 0) {
              if (e instanceof CelebornIOException) {
                callback.onFailure(e);
              } else {
                callback.onFailure(new CelebornIOException(cause, e));
              }
              return;
            }
            logger.error(
                "Push merged data to {} failed for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}, remain revive times {}.",
                addressPair,
                shuffleId,
                mapId,
                attemptId,
                Arrays.toString(partitionIds),
                groupedBatchId,
                Arrays.toString(batchIds),
                remainReviveTimes,
                e);
            if (!mapperEnded(shuffleId, mapId)) {
              ReviveRequest[] requests =
                  addAndGetReviveRequests(shuffleId, mapId, attemptId, batches, cause);
              presetSuccessIfAnotherUsable(shuffleId, mapId, requests, cause);
              pushDataRetryPool.submit(
                  () ->
                      submitRetryPushMergedData(
                          pushState,
                          shuffleId,
                          mapId,
                          attemptId,
                          batches,
                          cause,
                          groupedBatchId,
                          requests,
                          remainReviveTimes - 1,
                          System.currentTimeMillis()
                              + conf.clientRpcRequestPartitionLocationAskTimeout()
                                  .duration()
                                  .toMillis()));
            } else {
              pushState.removeBatch(groupedBatchId, hostPort);
              logger.info(
                  "Push merged data to {} failed but mapper already ended for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}, remain revive times {}.",
                  hostPort,
                  shuffleId,
                  mapId,
                  attemptId,
                  Arrays.toString(partitionIds),
                  groupedBatchId,
                  Arrays.toString(batchIds),
                  remainReviveTimes);
            }
          }
        };

    // do push merged data
    try {
      if (!isPushTargetWorkerExcluded(batches.get(0).loc, wrappedCallback)) {
        if (!testRetryRevive || remainReviveTimes < 1) {
          assert dataClientFactory != null;
          TransportClient client = dataClientFactory.createClient(host, port);
          client.pushMergedData(mergedData, pushDataTimeout, wrappedCallback);
        } else {
          wrappedCallback.onFailure(
              new CelebornIOException(
                  StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_PRIMARY,
                  new RuntimeException("Mock push merge data failed.")));
        }
      }
    } catch (Exception e) {
      logger.error(
          "Exception raised while pushing merged data for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {} location {}.",
          shuffleId,
          mapId,
          attemptId,
          Arrays.toString(partitionIds),
          groupedBatchId,
          Arrays.toString(batchIds),
          addressPair,
          e);
      if (e instanceof InterruptedException) {
        wrappedCallback.onFailure(e);
      } else {
        wrappedCallback.onFailure(
            new CelebornIOException(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY, e));
      }
    }
  }

  @Override
  public void mapperEnd(int shuffleId, int mapId, int attemptId, int numMappers, int numPartitions)
      throws IOException {
    mapEndInternal(shuffleId, mapId, attemptId, numMappers, numPartitions, -1);
  }

  @Override
  public void mapPartitionMapperEnd(
      int shuffleId, int mapId, int attemptId, int numMappers, int numPartitions, int partitionId)
      throws IOException {
    mapEndInternal(shuffleId, mapId, attemptId, numMappers, numPartitions, partitionId);
  }

  private void mapEndInternal(
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int numPartitions,
      Integer partitionId)
      throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = getPushState(mapKey);

    try {
      limitZeroInFlight(mapKey, pushState);

      // send CRC32 and num bytes per partition if e2e checks are enabled
      int[] crc32PerPartition =
          pushState.getCRC32PerPartition(shuffleIntegrityCheckEnabled, numPartitions);
      long[] bytesPerPartition =
          pushState.getBytesWrittenPerPartition(shuffleIntegrityCheckEnabled, numPartitions);

      MapperEndResponse response =
          lifecycleManagerRef.askSync(
              new MapperEnd(
                  shuffleId,
                  mapId,
                  attemptId,
                  numMappers,
                  partitionId,
                  pushState.getFailedBatches(),
                  numPartitions,
                  crc32PerPartition,
                  bytesPerPartition,
                  SerdeVersion.V1),
              rpcMaxRetries,
              rpcRetryWait,
              ClassTag$.MODULE$.apply(MapperEndResponse.class));
      if (response.status() != StatusCode.SUCCESS) {
        throw new CelebornIOException("MapperEnd failed! StatusCode: " + response.status());
      }
    } finally {
      logAndClearPushStats(mapKey, shuffleId, mapId, attemptId, pushState);
      pushStates.remove(mapKey);
    }
  }

  @Override
  public void cleanup(int shuffleId, int mapId, int attemptId) {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = pushStates.remove(mapKey);
    if (pushState != null) {
      pushState.exception.compareAndSet(null, new CelebornIOException("Cleaned Up"));
      pushState.cleanup();
    }
    logAndClearPushStats(mapKey, shuffleId, mapId, attemptId, pushState);
  }

  @Override
  public boolean cleanupShuffle(int shuffleId) {
    // clear status
    reducePartitionMap.remove(shuffleId);
    reduceFileGroupsMap.remove(shuffleId);
    fileGroupLoadLocks.remove(shuffleId);
    mapperEndMap.remove(shuffleId);
    stageEndShuffleSet.remove(shuffleId);
    splitting.remove(shuffleId);
    pushEventStats.keySet().removeIf(mapKey -> mapKey.startsWith(shuffleId + "-"));

    logger.info("Unregistered shuffle {}.", shuffleId);
    return true;
  }

  protected Tuple3<ReduceFileGroups, String, Exception> loadFileGroupInternal(
      int shuffleId, boolean isSegmentGranularityVisible) {
    long getReducerFileGroupStartTime = System.nanoTime();
    String exceptionMsg = null;
    Exception exception = null;
    if (lifecycleManagerRef == null) {
      exceptionMsg = "Driver endpoint is null!";
      logger.warn(exceptionMsg);
      return Tuple3.apply(null, exceptionMsg, exception);
    }
    try {
      GetReducerFileGroup getReducerFileGroup =
          new GetReducerFileGroup(shuffleId, isSegmentGranularityVisible, SerdeVersion.V1);

      GetReducerFileGroupResponse response =
          lifecycleManagerRef.askSync(
              getReducerFileGroup,
              conf.clientRpcGetReducerFileGroupAskTimeout(),
              rpcMaxRetries,
              rpcRetryWait,
              ClassTag$.MODULE$.apply(GetReducerFileGroupResponse.class));
      switch (response.status()) {
        case SUCCESS:
          if (response.broadcast() != null && response.broadcast().length > 0) {
            response =
                ShuffleClient.deserializeReducerFileGroupResponse(shuffleId, response.broadcast());
            if (response == null) {
              throw new CelebornBroadcastException(
                  "Failed to get GetReducerFileGroupResponse broadcast for shuffle: " + shuffleId);
            }
          }
          logger.info(
              "Shuffle {} request reducer file group success using {} ms, result partition size {}.",
              shuffleId,
              TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - getReducerFileGroupStartTime),
              response.fileGroup().size());
          return Tuple3.apply(
              new ReduceFileGroups(
                  response.fileGroup(),
                  response.attempts(),
                  response.partitionIds(),
                  response.pushFailedBatches()),
              null,
              null);
        case SHUFFLE_UNREGISTERED:
          logger.warn(
              "Request {} return {} for {}.", getReducerFileGroup, response.status(), shuffleId);
          // return empty result
          return Tuple3.apply(
              new ReduceFileGroups(
                  response.fileGroup(),
                  response.attempts(),
                  response.partitionIds(),
                  response.pushFailedBatches()),
              null,
              null);
        case STAGE_END_TIMEOUT:
        case SHUFFLE_DATA_LOST:
          exceptionMsg =
              String.format(
                  "Request %s return %s for %s.",
                  getReducerFileGroup, response.status(), shuffleId);
          logger.warn(exceptionMsg);
          break;
        default: // fall out
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      logger.error("Exception raised while call GetReducerFileGroup for {}.", shuffleId, e);
      exceptionMsg = e.getMessage();
      exception = e;
    }
    return Tuple3.apply(null, exceptionMsg, exception);
  }

  @Override
  public ReduceFileGroups updateFileGroup(int shuffleId, int partitionId)
      throws CelebornIOException {
    return updateFileGroup(shuffleId, partitionId, false);
  }

  @Override
  public boolean isShuffleStageEnd(int shuffleId) throws Exception {
    if (null != lifecycleManagerRef) {
      PbGetStageEnd request = PbGetStageEnd.newBuilder().setShuffleId(shuffleId).build();
      PbGetStageEndResponse response =
          lifecycleManagerRef.askSync(
              request,
              rpcMaxRetries,
              rpcRetryWait,
              ClassTag$.MODULE$.apply(PbGetStageEndResponse.class));
      return response.getStageEnd();
    } else {
      throw new RuntimeException("Driver endpoint is null!");
    }
  }

  public ReduceFileGroups updateFileGroup(
      int shuffleId, int partitionId, boolean isSegmentGranularityVisible)
      throws CelebornIOException {
    // Cache hits take no lock. compute() would hold the bin lock across the blocking RPC and
    // convoy every reduce task of the shuffle, so cold loads serialize on a per-shuffle monitor
    // (double-checked) instead: one RPC in flight, and the RPC runs off the map's bin lock.
    Tuple3<ReduceFileGroups, String, Exception> fileGroupTuple = reduceFileGroupsMap.get(shuffleId);
    if (fileGroupTuple == null || fileGroupTuple._1() == null) {
      synchronized (fileGroupLoadLocks.computeIfAbsent(shuffleId, id -> new Object())) {
        fileGroupTuple = reduceFileGroupsMap.get(shuffleId);
        if (fileGroupTuple == null || fileGroupTuple._1() == null) {
          fileGroupTuple = loadFileGroupInternal(shuffleId, isSegmentGranularityVisible);
          reduceFileGroupsMap.put(shuffleId, fileGroupTuple);
        }
      }
    }
    if (fileGroupTuple._1() == null) {
      throw new CelebornIOException(
          loadFileGroupException(shuffleId, partitionId, (fileGroupTuple._2())),
          fileGroupTuple._3());
    } else {
      return fileGroupTuple._1();
    }
  }

  protected String loadFileGroupException(int shuffleId, int partitionId, String exceptionMsg) {
    return String.format(
        "Failed to load file group of shuffle %d partition %d! %s",
        shuffleId,
        partitionId,
        StringUtils.isEmpty(exceptionMsg) ? StringUtils.EMPTY : exceptionMsg);
  }

  @Override
  public CelebornInputStream readPartition(
      int shuffleId,
      int appShuffleId,
      int partitionId,
      int attemptNumber,
      long taskId,
      int startMapIndex,
      int endMapIndex,
      ExceptionMaker exceptionMaker,
      ArrayList<PartitionLocation> locations,
      ArrayList<PbStreamHandler> streamHandlers,
      Map<String, LocationPushFailedBatches> failedBatchSetMap,
      Map<String, Pair<Integer, Integer>> chunksRange,
      int[] mapAttempts,
      MetricsCallback metricsCallback,
      boolean needDecompress)
      throws IOException {
    if (shuffleId == Utils$.MODULE$.UNKNOWN_APP_SHUFFLE_ID()) {
      logger.warn("Shuffle data is empty for shuffle {}: UNKNOWN_APP_SHUFFLE_ID.", shuffleId);
      return CelebornInputStream.empty();
    }

    // When `mapAttempts` is not null, it's guaranteed that the code path comes from
    // CelebornShuffleReader, which means `updateFileGroup` is already called and
    // batch open stream has been tried
    if (mapAttempts == null) {
      ReduceFileGroups fileGroups = updateFileGroup(shuffleId, partitionId, false);
      mapAttempts = fileGroups.mapAttempts;
      if (fileGroups.partitionGroups.containsKey(partitionId)) {
        locations = new ArrayList(fileGroups.partitionGroups.get(partitionId));
      }
    }

    if (locations == null || locations.size() == 0) {
      logger.warn("Shuffle data is empty for shuffle {} partition {}.", shuffleId, partitionId);
      return CelebornInputStream.empty();
    } else {
      String shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId);
      assert dataClientFactory != null;
      return CelebornInputStream.create(
          conf,
          dataClientFactory,
          shuffleKey,
          locations,
          streamHandlers,
          mapAttempts,
          failedBatchSetMap,
          chunksRange,
          attemptNumber,
          taskId,
          startMapIndex,
          endMapIndex,
          fetchExcludedWorkers,
          this,
          appShuffleId,
          shuffleId,
          partitionId,
          exceptionMaker,
          metricsCallback,
          needDecompress,
          cryptoHandler);
    }
  }

  @VisibleForTesting
  public Map<Integer, Tuple3<ReduceFileGroups, String, Exception>> getReduceFileGroupsMap() {
    return reduceFileGroupsMap;
  }

  @Override
  public void shutdown() {
    if (null != reviveManager) {
      reviveManager.close();
    }
    if (null != rpcEnv) {
      rpcEnv.shutdown();
    }
    if (null != dataClientFactory) {
      dataClientFactory.close();
    }
    if (null != transportContext) {
      transportContext.close();
    }
    if (null != pushDataRetryPool) {
      pushDataRetryPool.shutdown();
    }
    if (null != lifecycleManagerRef) {
      lifecycleManagerRef = null;
    }

    shuffleIdCache.clear();
    pushExcludedWorkers.clear();
    fetchExcludedWorkers.clear();
    messagesHelper.close();
    logger.warn("Shuffle client has been shutdown!");
  }

  @Override
  public void setupLifecycleManagerRef(String host, int port) {
    logger.info("setupLifecycleManagerRef: host = {}, port = {}", host, port);
    try {
      lifecycleManagerRef =
          rpcEnv.setupEndpointRef(
              new RpcAddress(host, port),
              RpcNameConstants.LIFECYCLE_MANAGER_EP,
              rpcMaxRetries,
              rpcRetryWait);
    } catch (Exception e) {
      throw new CelebornRuntimeException("setupLifecycleManagerRef failed!", e);
    }

    initDataClientFactoryIfNeeded();
  }

  @Override
  public void setupLifecycleManagerRef(RpcEndpointRef endpointRef) {
    lifecycleManagerRef = endpointRef;
    initDataClientFactoryIfNeeded();
  }

  @Override
  public void setExtension(byte[] extension) {
    this.extension = extension;
  }

  @Override
  public void setupCryptoHandler(Optional<CryptoHandler> cryptoHandler) {
    this.cryptoHandler = cryptoHandler != null ? cryptoHandler : Optional.empty();
    if (this.cryptoHandler.isPresent()) {
      logger.info("IO encryption enabled for shuffle data (encryption at rest).");
    }
  }

  private enum PushEvent {
    SOFT_SPLIT,
    HARD_SPLIT,
    PRIMARY_CONGESTED,
    REPLICA_CONGESTED
  }

  private static class PushEventCounters {
    private final LongAdder softSplitCount = new LongAdder();
    private final LongAdder hardSplitCount = new LongAdder();
    private final LongAdder primaryCongestedCount = new LongAdder();
    private final LongAdder replicaCongestedCount = new LongAdder();
  }

  private void recordPushEvent(
      int shuffleId, int mapId, int attemptId, String hostAndPushPort, PushEvent event) {
    PushEventCounters counters =
        pushEventStats
            .computeIfAbsent(
                Utils.makeMapKey(shuffleId, mapId, attemptId), k -> new ConcurrentHashMap<>())
            .computeIfAbsent(hostAndPushPort, k -> new PushEventCounters());
    switch (event) {
      case SOFT_SPLIT:
        counters.softSplitCount.increment();
        break;
      case HARD_SPLIT:
        counters.hardSplitCount.increment();
        break;
      case PRIMARY_CONGESTED:
        counters.primaryCongestedCount.increment();
        break;
      case REPLICA_CONGESTED:
        counters.replicaCongestedCount.increment();
        break;
    }
  }

  private void logAndClearPushStats(
      String mapKey, int shuffleId, int mapId, int attemptId, PushState pushState) {
    ConcurrentHashMap<String, PushEventCounters> workerStats = pushEventStats.remove(mapKey);
    long queueWaitMs = 0;
    long inflightWaitMs = 0;
    long drainWaitMs = 0;
    long slowPushes = 0;
    long maxPushRttMs = 0;
    if (pushState != null) {
      queueWaitMs = pushState.getQueueWaitTimeMs();
      inflightWaitMs = pushState.getInflightWaitTimeMs();
      drainWaitMs = pushState.getDrainWaitTimeMs();
      slowPushes = pushState.getSlowPushCount();
      maxPushRttMs = pushState.getMaxPushRttMs();
    }
    boolean hasEvents = workerStats != null && !workerStats.isEmpty();
    boolean hasSlowWrite = slowPushes > 0 || queueWaitMs + inflightWaitMs + drainWaitMs >= 1000;
    if (!hasEvents && !hasSlowWrite) {
      return;
    }
    StringBuilder sb = new StringBuilder();
    if (hasEvents) {
      workerStats.forEach(
          (worker, counters) ->
              sb.append("(")
                  .append(worker)
                  .append(", softSplit=")
                  .append(counters.softSplitCount.sum())
                  .append(", hardSplit=")
                  .append(counters.hardSplitCount.sum())
                  .append(", primaryCongested=")
                  .append(counters.primaryCongestedCount.sum())
                  .append(", replicaCongested=")
                  .append(counters.replicaCongestedCount.sum())
                  .append(")"));
    }
    logger.info(
        "Write stats summary for shuffle {} map {} attempt {}: queueWait={}ms, "
            + "inflightWait={}ms, drainWait={}ms, slowPush(>{}ms)={}, maxPushRtt={}ms, "
            + "events=[{}]",
        shuffleId,
        mapId,
        attemptId,
        queueWaitMs,
        inflightWaitMs,
        drainWaitMs,
        conf.clientPushSlowPushThresholdMs(),
        slowPushes,
        maxPushRttMs,
        sb);
  }

  boolean mapperEnded(int shuffleId, int mapId) {
    return (mapperEndMap.containsKey(shuffleId) && mapperEndMap.get(shuffleId).contains(mapId))
        || isStageEnded(shuffleId);
  }

  protected boolean isStageEnded(int shuffleId) {
    return stageEndShuffleSet.contains(shuffleId);
  }

  private StatusCode getPushDataFailCause(String message) {
    logger.debug("Push data failed cause message: {}", message);
    StatusCode cause;
    if (message == null) {
      logger.error("Push data throw unexpected exception");
      cause = StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_PRIMARY;
    } else if (message.startsWith(StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_REPLICA.name())) {
      cause = StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_REPLICA;
    } else if (message.startsWith(StatusCode.PUSH_DATA_WRITE_FAIL_REPLICA.name())) {
      cause = StatusCode.PUSH_DATA_WRITE_FAIL_REPLICA;
    } else if (message.startsWith(StatusCode.PUSH_DATA_WRITE_FAIL_PRIMARY.name())) {
      cause = StatusCode.PUSH_DATA_WRITE_FAIL_PRIMARY;
    } else if (message.startsWith(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY.name())) {
      cause = StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY;
    } else if (message.startsWith(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_REPLICA.name())) {
      cause = StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_REPLICA;
    } else if (message.startsWith(StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY.name())) {
      cause = StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY;
    } else if (message.startsWith(StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_REPLICA.name())) {
      cause = StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_REPLICA;
    } else if (message.startsWith(StatusCode.PUSH_DATA_TIMEOUT_PRIMARY.name())) {
      cause = StatusCode.PUSH_DATA_TIMEOUT_PRIMARY;
    } else if (message.startsWith(StatusCode.PUSH_DATA_TIMEOUT_REPLICA.name())) {
      cause = StatusCode.PUSH_DATA_TIMEOUT_REPLICA;
    } else if (message.startsWith(StatusCode.REPLICATE_DATA_FAILED.name())) {
      cause = StatusCode.REPLICATE_DATA_FAILED;
    } else if (message.startsWith(StatusCode.PUSH_DATA_PRIMARY_WORKER_EXCLUDED.name())) {
      cause = StatusCode.PUSH_DATA_PRIMARY_WORKER_EXCLUDED;
    } else if (message.startsWith(StatusCode.PUSH_DATA_REPLICA_WORKER_EXCLUDED.name())) {
      cause = StatusCode.PUSH_DATA_REPLICA_WORKER_EXCLUDED;
    } else if (message.startsWith(StatusCode.PUSH_DATA_FAIL_PARTITION_NOT_FOUND.name())) {
      cause = StatusCode.PUSH_DATA_FAIL_PARTITION_NOT_FOUND;
    } else if (ExceptionUtils.connectFail(message)) {
      // Throw when push to primary worker connection causeException.
      cause = StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY;
    } else {
      cause = StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_PRIMARY;
    }
    return cause;
  }

  @VisibleForTesting
  @Override
  public TransportClientFactory getDataClientFactory() {
    return dataClientFactory;
  }

  @Override
  public void excludeFailedFetchLocation(String hostAndFetchPort, Exception e) {
    if (pushReplicateEnabled
        && fetchExcludeWorkerOnFailureEnabled
        && Utils.isCriticalCauseForFetch(e)) {
      fetchExcludedWorkers.put(hostAndFetchPort, System.currentTimeMillis());
      logger.info(
          "Excluded fetch location {} due to fetch failure, {} excluded fetch locations in total.",
          hostAndFetchPort,
          fetchExcludedWorkers.size(),
          e);
    }
  }
}
