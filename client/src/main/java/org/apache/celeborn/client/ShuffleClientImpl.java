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
import java.util.concurrent.TimeoutException;

import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.compress.Compressor;
import org.apache.celeborn.client.read.RssInputStream;
import org.apache.celeborn.client.write.DataBatches;
import org.apache.celeborn.client.write.PushState;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.haclient.RssHARetryClient;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.PushData;
import org.apache.celeborn.common.network.protocol.PushDataHandShake;
import org.apache.celeborn.common.network.protocol.PushMergedData;
import org.apache.celeborn.common.network.protocol.RegionFinish;
import org.apache.celeborn.common.network.protocol.RegionStart;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.*;
import org.apache.celeborn.common.protocol.message.ControlMessages;
import org.apache.celeborn.common.protocol.message.ControlMessages.*;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.rpc.RpcAddress;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.PackedPartitionId;
import org.apache.celeborn.common.util.PbSerDeUtils;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.common.util.Utils;

public class ShuffleClientImpl extends ShuffleClient {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleClientImpl.class);

  private static final byte MASTER_MODE = PartitionLocation.Mode.MASTER.mode();

  private static final Random RND = new Random();

  private final CelebornConf conf;

  private final UserIdentifier userIdentifier;

  private final int registerShuffleMaxRetries;
  private final long registerShuffleRetryWaitMs;
  private final int maxInFlight;
  private final int pushBufferMaxSize;

  private final RpcEnv rpcEnv;

  private RpcEndpointRef driverRssMetaService;

  protected TransportClientFactory dataClientFactory;

  // key: shuffleId, value: (partitionId, PartitionLocation)
  private final Map<Integer, ConcurrentHashMap<Integer, PartitionLocation>> reducePartitionMap =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<Integer, Set<String>> mapperEndMap = new ConcurrentHashMap<>();

  // key: shuffleId-mapId-attemptId
  private final Map<String, PushState> pushStates = new ConcurrentHashMap<>();

  private final ExecutorService pushDataRetryPool;

  private final ExecutorService partitionSplitPool;
  private final Map<Integer, Set<Integer>> splitting = new ConcurrentHashMap<>();

  ThreadLocal<Compressor> compressorThreadLocal =
      new ThreadLocal<Compressor>() {
        @Override
        protected Compressor initialValue() {
          return Compressor.getCompressor(conf);
        }
      };

  private static class ReduceFileGroups {
    final PartitionLocation[][] partitionGroups;
    final int[] mapAttempts;

    ReduceFileGroups(PartitionLocation[][] partitionGroups, int[] mapAttempts) {
      this.partitionGroups = partitionGroups;
      this.mapAttempts = mapAttempts;
    }
  }

  // key: shuffleId
  private final Map<Integer, ReduceFileGroups> reduceFileGroupsMap = new ConcurrentHashMap<>();

  public ShuffleClientImpl(CelebornConf conf, UserIdentifier userIdentifier) {
    super();
    this.conf = conf;
    this.userIdentifier = userIdentifier;
    registerShuffleMaxRetries = conf.registerShuffleMaxRetry();
    registerShuffleRetryWaitMs = conf.registerShuffleRetryWaitMs();
    maxInFlight = conf.pushMaxReqsInFlight();
    pushBufferMaxSize = conf.pushBufferMaxSize();

    // init rpc env and master endpointRef
    rpcEnv = RpcEnv.create("ShuffleClient", Utils.localHostName(), 0, conf);

    String module = TransportModuleConstants.DATA_MODULE;
    TransportConf dataTransportConf =
        Utils.fromCelebornConf(conf, module, conf.getInt("celeborn" + module + ".io.threads", 8));
    TransportContext context =
        new TransportContext(dataTransportConf, new BaseMessageHandler(), true);
    dataClientFactory = context.createClientFactory();

    int pushDataRetryThreads = conf.pushRetryThreads();
    pushDataRetryPool =
        ThreadUtils.newDaemonCachedThreadPool("celeborn-retry-sender", pushDataRetryThreads, 60);

    int pushSplitPartitionThreads = conf.pushSplitPartitionThreads();
    partitionSplitPool =
        ThreadUtils.newDaemonCachedThreadPool(
            "celeborn-shuffle-split", pushSplitPartitionThreads, 60);
  }

  private void submitRetryPushData(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      byte[] body,
      int batchId,
      PartitionLocation loc,
      RpcResponseCallback callback,
      PushState pushState,
      StatusCode cause) {
    int partitionId = loc.getId();
    if (!revive(
        applicationId, shuffleId, mapId, attemptId, partitionId, loc.getEpoch(), loc, cause)) {
      callback.onFailure(new IOException("Revive Failed"));
    } else if (mapperEnded(shuffleId, mapId, attemptId)) {
      logger.debug(
          "Retrying push data, but the mapper(map {} attempt {}) has ended.", mapId, attemptId);
      pushState.removeBatch(batchId);
    } else {
      PartitionLocation newLoc = reducePartitionMap.get(shuffleId).get(partitionId);
      logger.info("Revive success, new location for reduce {} is {}.", partitionId, newLoc);
      try {
        TransportClient client =
            dataClientFactory.createClient(newLoc.getHost(), newLoc.getPushPort(), partitionId);
        NettyManagedBuffer newBuffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(body));
        String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);

        PushData newPushData =
            new PushData(MASTER_MODE, shuffleKey, newLoc.getUniqueId(), newBuffer);
        ChannelFuture future = client.pushData(newPushData, callback);
        pushState.pushStarted(batchId, future, callback);
      } catch (Exception ex) {
        logger.warn(
            "Exception raised while pushing data for shuffle {} map {} attempt {}" + " batch {}.",
            shuffleId,
            mapId,
            attemptId,
            batchId,
            ex);
        callback.onFailure(ex);
      }
    }
  }

  private void submitRetryPushMergedData(
      PushState pushState,
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      ArrayList<DataBatches.DataBatch> batches,
      StatusCode cause,
      Integer oldGroupedBatchId) {
    HashMap<String, DataBatches> newDataBatchesMap = new HashMap<>();
    for (DataBatches.DataBatch batch : batches) {
      int partitionId = batch.loc.getId();
      if (!revive(
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          batch.loc.getEpoch(),
          batch.loc,
          cause)) {
        pushState.exception.compareAndSet(
            null,
            new IOException("Revive Failed in retry push merged data for location: " + batch.loc));
        return;
      } else if (mapperEnded(shuffleId, mapId, attemptId)) {
        logger.debug(
            "Retrying push data, but the mapper(map {} attempt {}) has ended.", mapId, attemptId);
      } else {
        PartitionLocation newLoc = reducePartitionMap.get(shuffleId).get(partitionId);
        logger.info("Revive success, new location for reduce {} is {}.", partitionId, newLoc);
        DataBatches newDataBatches =
            newDataBatchesMap.computeIfAbsent(genAddressPair(newLoc), (s) -> new DataBatches());
        newDataBatches.addDataBatch(newLoc, batch.batchId, batch.body);
      }
    }

    for (Map.Entry<String, DataBatches> entry : newDataBatchesMap.entrySet()) {
      String addressPair = entry.getKey();
      DataBatches newDataBatches = entry.getValue();
      String[] tokens = addressPair.split("-");
      doPushMergedData(
          tokens[0],
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          newDataBatches.requireBatches(),
          pushState,
          true);
    }
    pushState.removeBatch(oldGroupedBatchId);
  }

  private String genAddressPair(PartitionLocation loc) {
    String addressPair;
    if (loc.getPeer() != null) {
      addressPair = loc.hostAndPushPort() + "-" + loc.getPeer().hostAndPushPort();
    } else {
      addressPair = loc.hostAndPushPort();
    }
    return addressPair;
  }

  private ConcurrentHashMap<Integer, PartitionLocation> registerShuffle(
      String appId, int shuffleId, int numMappers, int numPartitions) {
    return registerShuffleInternal(
        shuffleId,
        numMappers,
        numMappers,
        () ->
            driverRssMetaService.askSync(
                RegisterShuffle$.MODULE$.apply(appId, shuffleId, numMappers, numPartitions),
                conf.registerShuffleRpcAskTimeout(),
                ClassTag$.MODULE$.apply(PbRegisterShuffleResponse.class)));
  }

  @VisibleForTesting
  public PartitionLocation registerMapPartitionTask(
      String appId, int shuffleId, int numMappers, int mapId, int attemptId) {
    int partitionId = PackedPartitionId.packedPartitionId(mapId, attemptId);
    logger.info(
        "register mapPartitionTask, mapId: {}, attemptId: {}, partitionId: {}",
        mapId,
        attemptId,
        partitionId);
    ConcurrentHashMap<Integer, PartitionLocation> partitionLocationMap =
        registerShuffleInternal(
            shuffleId,
            numMappers,
            numMappers,
            () ->
                driverRssMetaService.askSync(
                    RegisterMapPartitionTask$.MODULE$.apply(
                        appId, shuffleId, numMappers, mapId, attemptId, partitionId),
                    conf.registerShuffleRpcAskTimeout(),
                    ClassTag$.MODULE$.apply(PbRegisterShuffleResponse.class)));
    return partitionLocationMap.get(partitionId);
  }

  private ConcurrentHashMap<Integer, PartitionLocation> registerShuffleInternal(
      int shuffleId,
      int numMappers,
      int numPartitions,
      Callable<PbRegisterShuffleResponse> callable) {
    int numRetries = registerShuffleMaxRetries;
    while (numRetries > 0) {
      try {
        PbRegisterShuffleResponse response = callable.call();
        StatusCode respStatus = Utils.toStatusCode(response.getStatus());
        if (StatusCode.SUCCESS.equals(respStatus)) {
          ConcurrentHashMap<Integer, PartitionLocation> result = new ConcurrentHashMap<>();
          for (int i = 0; i < response.getPartitionLocationsList().size(); i++) {
            PartitionLocation partitionLoc =
                PbSerDeUtils.fromPbPartitionLocation(response.getPartitionLocationsList().get(i));
            result.put(partitionLoc.getId(), partitionLoc);
          }
          return result;
        } else if (StatusCode.SLOT_NOT_AVAILABLE.equals(respStatus)) {
          logger.warn(
              "LifecycleManager request slots return {}, retry again, remain retry times {}",
              StatusCode.SLOT_NOT_AVAILABLE,
              numRetries - 1);
        } else {
          logger.error(
              "LifecycleManager request slots return {}, retry again, remain retry times {}",
              StatusCode.REQUEST_FAILED,
              numRetries - 1);
        }
      } catch (Exception e) {
        logger.error(
            "Exception raised while registering shuffle {} with {} mapper and {} partitions.",
            shuffleId,
            numMappers,
            numPartitions,
            e);
        break;
      }

      try {
        TimeUnit.MILLISECONDS.sleep(registerShuffleRetryWaitMs);
      } catch (InterruptedException e) {
        break;
      }
      numRetries--;
    }

    return null;
  }

  private void limitMaxInFlight(String mapKey, PushState pushState, int limit) throws IOException {
    if (pushState.exception.get() != null) {
      throw pushState.exception.get();
    }

    long timeoutMs = conf.pushLimitInFlightTimeoutMs();
    long delta = conf.pushLimitInFlightSleepDeltaMs();
    long times = timeoutMs / delta;
    try {
      while (times > 0) {
        if (pushState.inflightBatchCount() <= limit) {
          break;
        }
        if (pushState.exception.get() != null) {
          throw pushState.exception.get();
        }

        pushState.failExpiredBatch();

        Thread.sleep(delta);
        times--;
      }
    } catch (InterruptedException e) {
      pushState.exception.set(new IOException(e));
    }

    if (times <= 0) {
      logger.error(
          "After waiting for {} ms, there are still {} batches in flight for map {}, "
              + "which exceeds the limit {}.",
          timeoutMs,
          pushState.inflightBatchCount(),
          mapKey,
          limit);
      throw new IOException("wait timeout for task " + mapKey, pushState.exception.get());
    }
    if (pushState.exception.get() != null) {
      throw pushState.exception.get();
    }
  }

  private boolean waitRevivedLocation(
      ConcurrentHashMap<Integer, PartitionLocation> map, int partitionId, int epoch) {
    PartitionLocation currentLocation = map.get(partitionId);
    if (currentLocation != null && currentLocation.getEpoch() > epoch) {
      return true;
    }

    long sleepTimeMs = RND.nextInt(50);
    if (sleepTimeMs > 30) {
      try {
        TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
      } catch (InterruptedException e) {
        logger.warn("Wait revived location interrupted", e);
        Thread.currentThread().interrupt();
      }
    }

    currentLocation = map.get(partitionId);
    return currentLocation != null && currentLocation.getEpoch() > epoch;
  }

  private boolean revive(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      int epoch,
      PartitionLocation oldLocation,
      StatusCode cause) {
    ConcurrentHashMap<Integer, PartitionLocation> map = reducePartitionMap.get(shuffleId);
    if (waitRevivedLocation(map, partitionId, epoch)) {
      logger.debug(
          "Has already revived for shuffle {} map {} reduce {} epoch {},"
              + " just return(Assume revive successfully).",
          shuffleId,
          mapId,
          partitionId,
          epoch);
      return true;
    }
    String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    if (mapperEnded(shuffleId, mapId, attemptId)) {
      logger.debug(
          "The mapper(shuffle {} map {}) has already ended, just return(Assume"
              + " revive successfully).",
          shuffleId,
          mapId);
      return true;
    }

    try {
      PbChangeLocationResponse response =
          driverRssMetaService.askSync(
              Revive$.MODULE$.apply(
                  applicationId,
                  shuffleId,
                  mapId,
                  attemptId,
                  partitionId,
                  epoch,
                  oldLocation,
                  cause),
              conf.requestPartitionLocationRpcAskTimeout(),
              ClassTag$.MODULE$.apply(PbChangeLocationResponse.class));
      // per partitionKey only serve single PartitionLocation in Client Cache.
      StatusCode respStatus = Utils.toStatusCode(response.getStatus());
      if (StatusCode.SUCCESS.equals(respStatus)) {
        map.put(partitionId, PbSerDeUtils.fromPbPartitionLocation(response.getLocation()));
        return true;
      } else if (StatusCode.MAP_ENDED.equals(respStatus)) {
        mapperEndMap.computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet()).add(mapKey);
        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      logger.error(
          "Exception raised while reviving for shuffle {} reduce {} epoch {}.",
          shuffleId,
          partitionId,
          epoch,
          e);
      return false;
    }
  }

  public int pushOrMergeData(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions,
      boolean doPush)
      throws IOException {
    // mapKey
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    final String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
    // return if shuffle stage already ended
    if (mapperEnded(shuffleId, mapId, attemptId)) {
      logger.debug(
          "The mapper(shuffle {} map {} attempt {}) has already ended while" + " pushing data.",
          shuffleId,
          mapId,
          attemptId);
      PushState pushState = pushStates.get(mapKey);
      if (pushState != null) {
        pushState.cleanup();
      }
      return 0;
    }
    // register shuffle if not registered
    final ConcurrentHashMap<Integer, PartitionLocation> map =
        reducePartitionMap.computeIfAbsent(
            shuffleId,
            (id) -> registerShuffle(applicationId, shuffleId, numMappers, numPartitions));

    if (map == null) {
      throw new IOException("Register shuffle failed for shuffle " + shuffleKey);
    }

    // get location
    if (!map.containsKey(partitionId)) {
      logger.warn("It should never reach here!");
      if (!revive(
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          -1,
          null,
          StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE)) {
        throw new IOException(
            "Revive for shuffle " + shuffleKey + " partitionId " + partitionId + " failed.");
      }
    }

    if (mapperEnded(shuffleId, mapId, attemptId)) {
      logger.debug(
          "The mapper(shuffle {} map {} attempt {}) has already ended while" + " pushing data.",
          shuffleId,
          mapId,
          attemptId);
      PushState pushState = pushStates.get(mapKey);
      if (pushState != null) {
        pushState.cleanup();
      }
      return 0;
    }

    final PartitionLocation loc = map.get(partitionId);
    if (loc == null) {
      throw new IOException(
          "Partition location for shuffle "
              + shuffleKey
              + " partitionId "
              + partitionId
              + " is NULL!");
    }

    PushState pushState = pushStates.computeIfAbsent(mapKey, (s) -> new PushState(conf));

    // increment batchId
    final int nextBatchId = pushState.batchId.addAndGet(1);

    // compress data
    final Compressor compressor = compressorThreadLocal.get();
    compressor.compress(data, offset, length);

    final int compressedTotalSize = compressor.getCompressedTotalSize();
    final int BATCH_HEADER_SIZE = 4 * 4;
    final byte[] body = new byte[BATCH_HEADER_SIZE + compressedTotalSize];
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET, mapId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 4, attemptId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 8, nextBatchId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 12, compressedTotalSize);
    System.arraycopy(
        compressor.getCompressedBuffer(), 0, body, BATCH_HEADER_SIZE, compressedTotalSize);

    if (doPush) {
      logger.debug(
          "Do push data for app {} shuffle {} map {} attempt {} reduce {} batch {}.",
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          nextBatchId);
      // check limit
      limitMaxInFlight(mapKey, pushState, maxInFlight);

      // add inFlight requests
      pushState.addBatch(nextBatchId);

      // build PushData request
      NettyManagedBuffer buffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(body));
      PushData pushData = new PushData(MASTER_MODE, shuffleKey, loc.getUniqueId(), buffer);

      // build callback
      RpcResponseCallback callback =
          new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
              pushState.removeBatch(nextBatchId);
              // TODO Need to adjust maxReqsInFlight if server response is congested, see
              // CELEBORN-62
              if (response.remaining() > 0 && response.get() == StatusCode.STAGE_ENDED.getValue()) {
                mapperEndMap
                    .computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet())
                    .add(mapKey);
              }
              logger.debug(
                  "Push data to {}:{} success for map {} attempt {} batch {}.",
                  loc.getHost(),
                  loc.getPushPort(),
                  mapId,
                  attemptId,
                  nextBatchId);
            }

            @Override
            public void onFailure(Throwable e) {
              pushState.exception.compareAndSet(
                  null, new IOException("Revived PushData failed!", e));
              logger.error(
                  "Push data to {}:{} failed for map {} attempt {} batch {}.",
                  loc.getHost(),
                  loc.getPushPort(),
                  mapId,
                  attemptId,
                  nextBatchId,
                  e);
            }
          };

      RpcResponseCallback wrappedCallback =
          new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
              if (response.remaining() > 0) {
                byte reason = response.get();
                if (reason == StatusCode.SOFT_SPLIT.getValue()) {
                  logger.debug(
                      "Push data split required for map {} attempt {} batch {}",
                      mapId,
                      attemptId,
                      nextBatchId);
                  splitPartition(shuffleId, partitionId, applicationId, loc);
                  callback.onSuccess(response);
                } else if (reason == StatusCode.HARD_SPLIT.getValue()) {
                  logger.debug(
                      "Push data split for map {} attempt {} batch {}.",
                      mapId,
                      attemptId,
                      nextBatchId);
                  pushDataRetryPool.submit(
                      () ->
                          submitRetryPushData(
                              applicationId,
                              shuffleId,
                              mapId,
                              attemptId,
                              body,
                              nextBatchId,
                              loc,
                              this,
                              pushState,
                              StatusCode.HARD_SPLIT));
                } else {
                  response.rewind();
                  callback.onSuccess(response);
                }
              } else {
                callback.onSuccess(response);
              }
            }

            @Override
            public void onFailure(Throwable e) {
              if (pushState.exception.get() != null) {
                return;
              }
              logger.error(
                  "Push data to {}:{} failed for map {} attempt {} batch {}.",
                  loc.getHost(),
                  loc.getPushPort(),
                  mapId,
                  attemptId,
                  nextBatchId,
                  e);
              // async retry push data
              if (!mapperEnded(shuffleId, mapId, attemptId)) {
                pushDataRetryPool.submit(
                    () ->
                        submitRetryPushData(
                            applicationId,
                            shuffleId,
                            mapId,
                            attemptId,
                            body,
                            nextBatchId,
                            loc,
                            callback,
                            pushState,
                            getPushDataFailCause(e.getMessage())));
              } else {
                pushState.removeBatch(nextBatchId);
                logger.info(
                    "Mapper shuffleId:{} mapId:{} attempt:{} already ended, remove batchId:{}.",
                    shuffleId,
                    mapId,
                    attemptId,
                    nextBatchId);
              }
            }
          };

      // do push data
      try {
        TransportClient client =
            dataClientFactory.createClient(loc.getHost(), loc.getPushPort(), partitionId);
        ChannelFuture future = client.pushData(pushData, wrappedCallback);
        pushState.pushStarted(nextBatchId, future, wrappedCallback);
      } catch (Exception e) {
        logger.warn("PushData failed", e);
        wrappedCallback.onFailure(
            new Exception(getPushDataFailCause(e.getMessage()).toString(), e));
      }
    } else {
      // add batch data
      logger.debug("Merge batch {}.", nextBatchId);
      String addressPair = genAddressPair(loc);
      boolean shouldPush = pushState.addBatchData(addressPair, loc, nextBatchId, body);
      if (shouldPush) {
        limitMaxInFlight(mapKey, pushState, maxInFlight);
        DataBatches dataBatches = pushState.takeDataBatches(addressPair);
        doPushMergedData(
            addressPair.split("-")[0],
            applicationId,
            shuffleId,
            mapId,
            attemptId,
            dataBatches.requireBatches(),
            pushState,
            false);
      }
    }

    return body.length;
  }

  private void splitPartition(
      int shuffleId, int partitionId, String applicationId, PartitionLocation loc) {
    Set<Integer> splittingSet =
        splitting.computeIfAbsent(shuffleId, integer -> ConcurrentHashMap.newKeySet());
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (splittingSet) {
      if (splittingSet.contains(partitionId)) {
        logger.debug(
            "shuffle {} partitionId {} is splitting, skip split request ", shuffleId, partitionId);
        return;
      }
      splittingSet.add(partitionId);
    }

    ConcurrentHashMap<Integer, PartitionLocation> currentShuffleLocs =
        reducePartitionMap.get(shuffleId);

    ShuffleClientHelper.sendShuffleSplitAsync(
        driverRssMetaService,
        conf,
        PartitionSplit$.MODULE$.apply(applicationId, shuffleId, partitionId, loc.getEpoch(), loc),
        partitionSplitPool,
        splittingSet,
        partitionId,
        shuffleId,
        currentShuffleLocs);
  }

  @Override
  public int pushData(
      String applicationId,
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
        applicationId,
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        data,
        offset,
        length,
        numMappers,
        numPartitions,
        true);
  }

  @Override
  public void prepareForMergeData(int shuffleId, int mapId, int attemptId) throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = pushStates.get(mapKey);
    if (pushState != null) {
      limitMaxInFlight(mapKey, pushState, 0);
    }
  }

  @Override
  public int mergeData(
      String applicationId,
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
        applicationId,
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        data,
        offset,
        length,
        numMappers,
        numPartitions,
        false);
  }

  public void pushMergedData(String applicationId, int shuffleId, int mapId, int attemptId)
      throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = pushStates.get(mapKey);
    if (pushState == null) {
      return;
    }
    ArrayList<Map.Entry<String, DataBatches>> batchesArr =
        new ArrayList<>(pushState.batchesMap.entrySet());
    while (!batchesArr.isEmpty()) {
      limitMaxInFlight(mapKey, pushState, maxInFlight);
      Map.Entry<String, DataBatches> entry = batchesArr.get(RND.nextInt(batchesArr.size()));
      ArrayList<DataBatches.DataBatch> batches = entry.getValue().requireBatches(pushBufferMaxSize);
      if (entry.getValue().getTotalSize() == 0) {
        batchesArr.remove(entry);
      }
      String[] tokens = entry.getKey().split("-");
      doPushMergedData(
          tokens[0], applicationId, shuffleId, mapId, attemptId, batches, pushState, false);
    }
  }

  private void doPushMergedData(
      String hostPort,
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      ArrayList<DataBatches.DataBatch> batches,
      PushState pushState,
      boolean revived) {
    final String[] splits = hostPort.split(":");
    final String host = splits[0];
    final int port = Integer.parseInt(splits[1]);

    int groupedBatchId = pushState.batchId.addAndGet(1);
    pushState.addBatch(groupedBatchId);

    final int numBatches = batches.size();
    final String[] partitionUniqueIds = new String[numBatches];
    final int[] offsets = new int[numBatches];
    final int[] batchIds = new int[numBatches];
    int currentSize = 0;
    CompositeByteBuf byteBuf = Unpooled.compositeBuffer();
    for (int i = 0; i < numBatches; i++) {
      DataBatches.DataBatch batch = batches.get(i);
      partitionUniqueIds[i] = batch.loc.getUniqueId();
      offsets[i] = currentSize;
      batchIds[i] = batch.batchId;
      currentSize += batch.body.length;
      byteBuf.addComponent(true, Unpooled.wrappedBuffer(batch.body));
    }
    NettyManagedBuffer buffer = new NettyManagedBuffer(byteBuf);
    String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
    PushMergedData mergedData =
        new PushMergedData(MASTER_MODE, shuffleKey, partitionUniqueIds, offsets, buffer);

    RpcResponseCallback callback =
        new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer response) {
            logger.debug(
                "Push data success for map {} attempt {} grouped batch {}.",
                mapId,
                attemptId,
                groupedBatchId);
            pushState.removeBatch(groupedBatchId);
            // TODO Need to adjust maxReqsInFlight if server response is congested, see CELEBORN-62
            if (response.remaining() > 0 && response.get() == StatusCode.STAGE_ENDED.getValue()) {
              mapperEndMap
                  .computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet())
                  .add(Utils.makeMapKey(shuffleId, mapId, attemptId));
            }
          }

          @Override
          public void onFailure(Throwable e) {
            String errorMsg =
                (revived ? "Revived push" : "Push")
                    + " merged data to "
                    + host
                    + ":"
                    + port
                    + " failed for map "
                    + mapId
                    + " attempt "
                    + attemptId
                    + " batches "
                    + Arrays.toString(batchIds)
                    + ".";
            pushState.exception.compareAndSet(null, new IOException(errorMsg, e));
            if (logger.isDebugEnabled()) {
              for (int batchId : batchIds) {
                logger.debug(
                    "Push data failed for map {} attempt {} batch {}.", mapId, attemptId, batchId);
              }
            }
          }
        };

    RpcResponseCallback wrappedCallback =
        new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer response) {
            if (response.remaining() > 0) {
              byte reason = response.get();
              if (reason == StatusCode.HARD_SPLIT.getValue()) {
                logger.info(
                    "Push merged data return hard split for map "
                        + mapId
                        + " attempt "
                        + attemptId
                        + " batches "
                        + Arrays.toString(batchIds)
                        + ".");
                pushDataRetryPool.submit(
                    () ->
                        submitRetryPushMergedData(
                            pushState,
                            applicationId,
                            shuffleId,
                            mapId,
                            attemptId,
                            batches,
                            StatusCode.HARD_SPLIT,
                            groupedBatchId));
              } else {
                // Should not happen in current architecture.
                response.rewind();
                logger.error("Push merged data should not receive this response");
                callback.onSuccess(response);
              }
            } else {
              callback.onSuccess(response);
            }
          }

          @Override
          public void onFailure(Throwable e) {
            if (pushState.exception.get() != null) {
              return;
            }
            if (revived) {
              callback.onFailure(e);
              return;
            }
            logger.error(
                "Push merged data to "
                    + host
                    + ":"
                    + port
                    + " failed for map "
                    + mapId
                    + " attempt "
                    + attemptId
                    + " batches "
                    + Arrays.toString(batchIds)
                    + ".",
                e);
            if (!mapperEnded(shuffleId, mapId, attemptId)) {
              pushDataRetryPool.submit(
                  () ->
                      submitRetryPushMergedData(
                          pushState,
                          applicationId,
                          shuffleId,
                          mapId,
                          attemptId,
                          batches,
                          getPushDataFailCause(e.getMessage()),
                          groupedBatchId));
            }
          }
        };

    // do push merged data
    try {
      TransportClient client = dataClientFactory.createClient(host, port);
      ChannelFuture future = client.pushMergedData(mergedData, wrappedCallback);
      pushState.pushStarted(groupedBatchId, future, wrappedCallback);
    } catch (Exception e) {
      logger.warn("PushMergedData failed", e);
      wrappedCallback.onFailure(new Exception(getPushDataFailCause(e.getMessage()).toString(), e));
    }
  }

  @Override
  public void mapperEnd(
      String applicationId, int shuffleId, int mapId, int attemptId, int numMappers)
      throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = pushStates.computeIfAbsent(mapKey, (s) -> new PushState(conf));

    try {
      limitMaxInFlight(mapKey, pushState, 0);

      MapperEndResponse response =
          driverRssMetaService.askSync(
              new MapperEnd(applicationId, shuffleId, mapId, attemptId, numMappers),
              ClassTag$.MODULE$.apply(MapperEndResponse.class));
      if (response.status() != StatusCode.SUCCESS) {
        throw new IOException("MapperEnd failed! StatusCode: " + response.status());
      }
    } finally {
      pushStates.remove(mapKey);
    }
  }

  @Override
  public void cleanup(String applicationId, int shuffleId, int mapId, int attemptId) {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = pushStates.remove(mapKey);
    if (pushState != null) {
      pushState.exception.compareAndSet(null, new IOException("Cleaned Up"));
      pushState.cleanup();
    }
  }

  @Override
  public boolean unregisterShuffle(String applicationId, int shuffleId, boolean isDriver) {
    if (isDriver) {
      try {
        driverRssMetaService.send(
            UnregisterShuffle$.MODULE$.apply(
                applicationId, shuffleId, RssHARetryClient.genRequestId()));
      } catch (Exception e) {
        // If some exceptions need to be ignored, they shouldn't be logged as error-level,
        // otherwise it will mislead users.
        logger.warn("Send UnregisterShuffle failed, ignore.", e);
      }
    }

    // clear status
    reducePartitionMap.remove(shuffleId);
    reduceFileGroupsMap.remove(shuffleId);
    mapperEndMap.remove(shuffleId);
    splitting.remove(shuffleId);

    logger.info("Unregistered shuffle {}.", shuffleId);
    return true;
  }

  @Override
  public RssInputStream readPartition(
      String applicationId, int shuffleId, int partitionId, int attemptNumber) throws IOException {
    return readPartition(
        applicationId, shuffleId, partitionId, attemptNumber, 0, Integer.MAX_VALUE);
  }

  @Override
  public RssInputStream readPartition(
      String applicationId,
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex)
      throws IOException {
    String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
    ReduceFileGroups fileGroups =
        reduceFileGroupsMap.computeIfAbsent(
            shuffleId,
            (id) -> {
              long getReducerFileGroupStartTime = System.nanoTime();
              try {
                if (driverRssMetaService == null) {
                  logger.warn("Driver endpoint is null!");
                  return null;
                }

                GetReducerFileGroup getReducerFileGroup =
                    new GetReducerFileGroup(applicationId, shuffleId);

                GetReducerFileGroupResponse response =
                    driverRssMetaService.askSync(
                        getReducerFileGroup,
                        conf.getReducerFileGroupRpcAskTimeout(),
                        ClassTag$.MODULE$.apply(GetReducerFileGroupResponse.class));

                if (response.status() == StatusCode.SUCCESS) {
                  logger.info(
                      "Shuffle {} request reducer file group success using time:{} ms",
                      shuffleId,
                      (System.nanoTime() - getReducerFileGroupStartTime) / 1000_000);
                  return new ReduceFileGroups(response.fileGroup(), response.attempts());
                } else if (response.status() == StatusCode.STAGE_END_TIME_OUT) {
                  logger.warn(
                      "Request {} return {} for {}",
                      getReducerFileGroup,
                      StatusCode.STAGE_END_TIME_OUT.toString(),
                      shuffleKey);
                } else if (response.status() == StatusCode.SHUFFLE_DATA_LOST) {
                  logger.warn(
                      "Request {} return {} for {}",
                      getReducerFileGroup,
                      StatusCode.SHUFFLE_DATA_LOST.toString(),
                      shuffleKey);
                }
              } catch (Exception e) {
                logger.error(
                    "Exception raised while call GetReducerFileGroup for " + shuffleKey + ".", e);
              }
              return null;
            });

    if (fileGroups == null) {
      String msg = "Shuffle data lost for shuffle " + shuffleId + " reduce " + partitionId + "!";
      logger.error(msg);
      throw new IOException(msg);
    } else if (fileGroups.partitionGroups.length == 0) {
      logger.warn("Shuffle data is empty for shuffle {} reduce {}.", shuffleId, partitionId);
      return RssInputStream.empty();
    } else {
      return RssInputStream.create(
          conf,
          dataClientFactory,
          shuffleKey,
          fileGroups.partitionGroups[partitionId],
          fileGroups.mapAttempts,
          attemptNumber,
          startMapIndex,
          endMapIndex);
    }
  }

  @Override
  public void shutdown() {
    if (null != rpcEnv) {
      rpcEnv.shutdown();
    }
    if (null != dataClientFactory) {
      dataClientFactory.close();
    }
    if (null != pushDataRetryPool) {
      pushDataRetryPool.shutdown();
    }
    if (null != partitionSplitPool) {
      partitionSplitPool.shutdown();
    }
    if (null != driverRssMetaService) {
      driverRssMetaService = null;
    }
    logger.warn("Shuffle client has been shutdown!");
  }

  @Override
  public void setupMetaServiceRef(String host, int port) {
    driverRssMetaService =
        rpcEnv.setupEndpointRef(new RpcAddress(host, port), RpcNameConstants.RSS_METASERVICE_EP);
  }

  @Override
  public void setupMetaServiceRef(RpcEndpointRef endpointRef) {
    driverRssMetaService = endpointRef;
  }

  private boolean mapperEnded(int shuffleId, int mapId, int attemptId) {
    return mapperEndMap.containsKey(shuffleId)
        && mapperEndMap.get(shuffleId).contains(Utils.makeMapKey(shuffleId, mapId, attemptId));
  }

  private StatusCode getPushDataFailCause(String message) {
    logger.info("[getPushDataFailCause] message: " + message);
    StatusCode cause;
    if (StatusCode.PUSH_DATA_FAIL_SLAVE.getMessage().equals(message)) {
      cause = StatusCode.PUSH_DATA_FAIL_SLAVE;
    } else if (StatusCode.PUSH_DATA_FAIL_MASTER.getMessage().equals(message)
        || connectFail(message)) {
      cause = StatusCode.PUSH_DATA_FAIL_MASTER;
    } else if (StatusCode.PUSH_DATA_TIMEOUT.getMessage().equals(message)) {
      cause = StatusCode.PUSH_DATA_TIMEOUT;
    } else {
      cause = StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE;
    }
    return cause;
  }

  private boolean connectFail(String message) {
    return (message.startsWith("Connection from ") && message.endsWith(" closed"))
        || (message.equals("Connection reset by peer"))
        || (message.startsWith("Failed to send RPC "));
  }

  @Override
  public void pushDataHandShake(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int numPartitions,
      int bufferSize,
      PartitionLocation location)
      throws IOException {
    sendMessageInternal(
        shuffleId,
        mapId,
        attemptId,
        () -> {
          String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
          logger.info(
              "pushDataHandShake shuffleKey:{}, attemptId:{}, locationId:{}",
              shuffleKey,
              attemptId,
              location.getUniqueId());
          logger.debug("pushDataHandShake location:{}", location.toString());
          TransportClient client =
              dataClientFactory.createClient(location.getHost(), location.getPushPort());
          PushDataHandShake handShake =
              new PushDataHandShake(
                  MASTER_MODE,
                  shuffleKey,
                  location.getUniqueId(),
                  attemptId,
                  numPartitions,
                  bufferSize);
          client.sendRpcSync(handShake.toByteBuffer(), conf.pushDataTimeoutMs());
          return null;
        });
  }

  @Override
  public Optional<PartitionLocation> regionStart(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      PartitionLocation location,
      int currentRegionIdx,
      boolean isBroadcast)
      throws IOException {
    return sendMessageInternal(
        shuffleId,
        mapId,
        attemptId,
        () -> {
          String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
          logger.info(
              "regionStart shuffleKey:{}, attemptId:{}, locationId:{}",
              shuffleKey,
              attemptId,
              location.getUniqueId());
          logger.debug("regionStart location:{}", location.toString());
          TransportClient client =
              dataClientFactory.createClient(location.getHost(), location.getPushPort());
          RegionStart regionStart =
              new RegionStart(
                  MASTER_MODE,
                  shuffleKey,
                  location.getUniqueId(),
                  attemptId,
                  currentRegionIdx,
                  isBroadcast);
          ByteBuffer regionStartResponse =
              client.sendRpcSync(regionStart.toByteBuffer(), conf.pushDataTimeoutMs());
          if (regionStartResponse.hasRemaining()
              && regionStartResponse.get() == StatusCode.HARD_SPLIT.getValue()) {
            // if split then revive
            PbChangeLocationResponse response =
                driverRssMetaService.askSync(
                    ControlMessages.Revive$.MODULE$.apply(
                        applicationId,
                        shuffleId,
                        mapId,
                        attemptId,
                        location.getId(),
                        location.getEpoch(),
                        location,
                        StatusCode.HARD_SPLIT),
                    conf.requestPartitionLocationRpcAskTimeout(),
                    ClassTag$.MODULE$.apply(PbChangeLocationResponse.class));
            // per partitionKey only serve single PartitionLocation in Client Cache.
            StatusCode respStatus = Utils.toStatusCode(response.getStatus());
            if (StatusCode.SUCCESS.equals(respStatus)) {
              return Optional.of(PbSerDeUtils.fromPbPartitionLocation(response.getLocation()));
            } else if (StatusCode.MAP_ENDED.equals(respStatus)) {
              final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
              mapperEndMap
                  .computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet())
                  .add(mapKey);
              return Optional.empty();
            } else {
              // throw exception
              logger.error(
                  "Exception raised while reviving for shuffle {} reduce {} epoch {}.",
                  shuffleId,
                  location.getId(),
                  location.getEpoch());
              throw new IOException("regiontstart revive failed");
            }
          }
          return Optional.empty();
        });
  }

  @Override
  public void regionFinish(
      String applicationId, int shuffleId, int mapId, int attemptId, PartitionLocation location)
      throws IOException {
    sendMessageInternal(
        shuffleId,
        mapId,
        attemptId,
        () -> {
          final String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
          logger.info(
              "regionFinish shuffleKey:{}, attemptId:{}, locationId:{}",
              shuffleKey,
              attemptId,
              location.getUniqueId());
          logger.debug("regionFinish location:{}", location.toString());
          TransportClient client =
              dataClientFactory.createClient(location.getHost(), location.getPushPort());
          RegionFinish regionFinish =
              new RegionFinish(MASTER_MODE, shuffleKey, location.getUniqueId(), attemptId);
          client.sendRpcSync(regionFinish.toByteBuffer(), conf.pushDataTimeoutMs());
          return null;
        });
  }

  private <R> R sendMessageInternal(
      int shuffleId, int mapId, int attemptId, ThrowingExceptionSupplier<R, Exception> supplier)
      throws IOException {
    PushState pushState = null;
    int batchId = 0;
    try {
      // mapKey
      final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
      // return if shuffle stage already ended
      if (mapperEnded(shuffleId, mapId, attemptId)) {
        logger.debug(
            "The mapper(shuffle {} map {} attempt {}) has already ended while" + " pushing data.",
            shuffleId,
            mapId,
            attemptId);
        return null;
      }
      pushState = pushStates.computeIfAbsent(mapKey, (s) -> new PushState(conf));
      // force data has been send
      limitMaxInFlight(mapKey, pushState, 0);

      // add inFlight requests
      batchId = pushState.batchId.incrementAndGet();
      pushState.addBatch(batchId);
      return retrySendMessage(supplier);
    } finally {
      if (pushState != null) {
        pushState.removeBatch(batchId);
      }
    }
  }

  @FunctionalInterface
  interface ThrowingExceptionSupplier<R, E extends Exception> {
    R get() throws E;
  }

  private <R> R retrySendMessage(ThrowingExceptionSupplier<R, Exception> supplier)
      throws IOException {

    int retryTimes = 0;
    boolean isSuccess = false;
    Exception currentException = null;
    R result = null;
    while (!Thread.currentThread().isInterrupted()
        && !isSuccess
        && retryTimes < conf.networkIoMaxRetries(TransportModuleConstants.PUSH_MODULE)) {
      logger.info("retrySendMessage times: {}", retryTimes);
      try {
        result = supplier.get();
        isSuccess = true;
      } catch (Exception e) {
        currentException = e;
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        if (shouldRetry(e)) {
          retryTimes++;
          Uninterruptibles.sleepUninterruptibly(
              conf.networkIoRetryWaitMs(TransportModuleConstants.PUSH_MODULE),
              TimeUnit.MILLISECONDS);
        } else {
          break;
        }
      }
    }
    if (!isSuccess) {
      if (currentException instanceof IOException) {
        throw (IOException) currentException;
      } else {
        throw new IOException(currentException.getMessage(), currentException);
      }
    }
    return result;
  }

  private boolean shouldRetry(Throwable e) {
    boolean isIOException =
        e instanceof IOException
            || e instanceof TimeoutException
            || (e.getCause() != null && e.getCause() instanceof TimeoutException)
            || (e.getCause() != null && e.getCause() instanceof IOException)
            || (e instanceof RuntimeException
                && e.getMessage().startsWith(IOException.class.getName()));
    return isIOException;
  }
}
