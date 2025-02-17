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

package org.apache.celeborn.plugin.flink;

import java.io.IOException;
import java.util.Optional;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.DriverChangedException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.plugin.flink.buffer.BufferHeader;
import org.apache.celeborn.plugin.flink.buffer.BufferPacker;
import org.apache.celeborn.plugin.flink.client.FlinkShuffleClientImpl;
import org.apache.celeborn.plugin.flink.utils.BufferUtils;
import org.apache.celeborn.plugin.flink.utils.Utils;

/**
 * A transportation gate used to spill buffers from {@link ResultPartitionWriter} to remote shuffle
 * worker. The whole process of communication between outputGate and shuffle worker could be
 * described as below:
 *
 * <ul>
 *   <li>1. Client registers shuffle to get partitionLocation which stores the data in remote
 *       shuffle;
 *   <li>2. Client sends PushDataHandShake to transfer handshake message;
 *   <li>3. Client sends RegionStart which announces the start of a writing region and maybe get a
 *       new partitionLocation;
 *   <li>4. Client write data;
 *   <li>5. Client sends RegionFinish to indicate writing finish of a region;
 *   <li>6. Repeat from step-2 to step-5;
 *   <li>7. Client sends mapend to indicate writing finish;
 * </ul>
 */
public class RemoteShuffleOutputGate {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleOutputGate.class);
  private final RemoteShuffleDescriptor shuffleDesc;
  protected final int numSubs;
  protected FlinkShuffleClientImpl flinkShuffleClient;
  protected final SupplierWithException<BufferPool, IOException> bufferPoolFactory;
  protected BufferPool bufferPool;
  private CelebornConf celebornConf;
  private final int numMappers;
  private PartitionLocation partitionLocation;

  private int currentRegionIndex = 0;

  private int bufferSize;
  private BufferPacker bufferPacker;
  private String applicationId;
  private int shuffleId;
  private int mapId;
  private int attemptId;
  private int partitionId;
  private String lifecycleManagerHost;
  private int lifecycleManagerPort;
  private long lifecycleManagerTimestamp;
  private UserIdentifier userIdentifier;
  private boolean isRegisterShuffle = false;
  private int maxReviveTimes;
  private boolean hasSentHandshake = false;

  /**
   * @param shuffleDesc Describes shuffle meta and shuffle worker address.
   * @param numSubs Number of subpartitions of the corresponding {@link ResultPartitionWriter}.
   * @param bufferPoolFactory {@link BufferPool} provider.
   */
  public RemoteShuffleOutputGate(
      RemoteShuffleDescriptor shuffleDesc,
      int numSubs,
      int bufferSize,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      CelebornConf celebornConf,
      int numMappers) {

    this.shuffleDesc = shuffleDesc;
    this.numSubs = numSubs;
    this.bufferPoolFactory = bufferPoolFactory;
    this.bufferPacker = new BufferPacker(this::write);
    this.celebornConf = celebornConf;
    this.numMappers = numMappers;
    this.bufferSize = bufferSize;
    this.applicationId = shuffleDesc.getCelebornAppId();
    this.shuffleId =
        shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getShuffleId();
    this.mapId = shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getMapId();
    this.attemptId =
        shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getAttemptId();
    this.partitionId =
        shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getPartitionId();
    this.lifecycleManagerHost = shuffleDesc.getShuffleResource().getLifecycleManagerHost();
    this.lifecycleManagerPort = shuffleDesc.getShuffleResource().getLifecycleManagerPort();
    this.lifecycleManagerTimestamp =
        shuffleDesc.getShuffleResource().getLifecycleManagerTimestamp();
    this.flinkShuffleClient = getShuffleClient();
    this.maxReviveTimes = celebornConf.clientPushMaxReviveTimes();
  }

  /** Initialize transportation gate. */
  public void setup() throws IOException, InterruptedException {
    bufferPool = Utils.checkNotNull(bufferPoolFactory.get());
    Utils.checkArgument(
        bufferPool.getNumberOfRequiredMemorySegments() >= 2,
        "Too few buffers for transfer, the minimum valid required size is 2.");

    // guarantee that we have at least one buffer
    BufferUtils.reserveNumRequiredBuffers(bufferPool, 1);
  }

  /** Get transportation buffer pool. */
  public BufferPool getBufferPool() {
    return bufferPool;
  }

  /** Writes a {@link Buffer} to a subpartition. */
  public void write(Buffer buffer, int subIdx) throws InterruptedException {
    bufferPacker.process(buffer, subIdx);
  }

  /**
   * Indicates the start of a region. A region of buffers guarantees the records inside are
   * completed.
   *
   * @param isBroadcast Whether it's a broadcast region.
   */
  public void regionStart(boolean isBroadcast) {
    try {
      registerShuffle();
      handshake();
      regionStartWithRevive(isBroadcast);
    } catch (IOException e) {
      Utils.rethrowAsRuntimeException(e);
    }
  }

  /**
   * Indicates the finish of a region. A region is always bounded by a pair of region-start and
   * region-finish.
   */
  public void regionFinish() throws InterruptedException {
    bufferPacker.drain();
    try {
      flinkShuffleClient.regionFinish(shuffleId, mapId, attemptId, partitionLocation);
      currentRegionIndex++;
    } catch (IOException e) {
      Utils.rethrowAsRuntimeException(e);
    }
  }

  /** Indicates the writing/spilling is finished. */
  public void finish() throws InterruptedException, IOException {
    flinkShuffleClient.mapPartitionMapperEnd(
        shuffleId, mapId, attemptId, numMappers, partitionLocation.getId());
  }

  /** Close the transportation gate. */
  public void close() throws IOException {
    if (bufferPool != null) {
      bufferPool.lazyDestroy();
    }
    bufferPacker.close();
    flinkShuffleClient.cleanup(shuffleId, mapId, attemptId);
  }

  /** Returns shuffle descriptor. */
  public RemoteShuffleDescriptor getShuffleDesc() {
    return shuffleDesc;
  }

  @VisibleForTesting
  FlinkShuffleClientImpl getShuffleClient() {
    try {
      return FlinkShuffleClientImpl.get(
          applicationId,
          lifecycleManagerHost,
          lifecycleManagerPort,
          lifecycleManagerTimestamp,
          celebornConf,
          userIdentifier);
    } catch (DriverChangedException e) {
      // would generate a new attempt to retry output gate
      throw new RuntimeException(e.getMessage());
    }
  }

  /** Writes a piece of data to a subpartition. */
  public void write(ByteBuf byteBuf, BufferHeader bufferHeader) {
    try {
      flinkShuffleClient.pushDataToLocation(
          shuffleId,
          mapId,
          attemptId,
          bufferHeader.getSubPartitionId(),
          io.netty.buffer.Unpooled.wrappedBuffer(byteBuf.nioBuffer()),
          partitionLocation,
          () -> byteBuf.release());
    } catch (IOException e) {
      Utils.rethrowAsRuntimeException(e);
    }
  }

  public void registerShuffle() throws IOException {
    if (!isRegisterShuffle) {
      partitionLocation =
          flinkShuffleClient.registerMapPartitionTask(
              shuffleId, numMappers, mapId, attemptId, partitionId);
      Utils.checkNotNull(partitionLocation);

      currentRegionIndex = 0;
      isRegisterShuffle = true;
    }
  }

  public void regionStartWithRevive(boolean isBroadcast) {
    try {
      int remainingReviveTimes = maxReviveTimes;
      boolean hasSentRegionStart = false;
      while (remainingReviveTimes-- > 0 && !hasSentRegionStart) {
        Optional<PartitionLocation> revivePartition =
            flinkShuffleClient.regionStart(
                shuffleId, mapId, attemptId, partitionLocation, currentRegionIndex, isBroadcast);
        if (revivePartition.isPresent()) {
          LOG.info(
              "Revive at regionStart, currentTimes:{}, totalTimes:{} for shuffleId:{}, mapId:{}, attempId:{}, currentRegionIndex:{}, isBroadcast:{}, newPartition:{}, oldPartition:{}",
              remainingReviveTimes,
              maxReviveTimes,
              shuffleId,
              mapId,
              attemptId,
              currentRegionIndex,
              isBroadcast,
              revivePartition,
              partitionLocation);
          partitionLocation = revivePartition.get();
          hasSentRegionStart = false;
          // For every revive partition, handshake should be sent firstly
          hasSentHandshake = false;
          handshake();
        } else {
          hasSentRegionStart = true;
        }
      }
      if (remainingReviveTimes == 0 && !hasSentRegionStart) {
        throw new RuntimeException(
            "After retry " + maxReviveTimes + " times, still failed to send regionStart");
      }
    } catch (IOException e) {
      Utils.rethrowAsRuntimeException(e);
    }
  }

  public void handshake() {
    try {
      int remainingReviveTimes = maxReviveTimes;
      while (remainingReviveTimes-- > 0 && !hasSentHandshake) {
        Optional<PartitionLocation> revivePartition =
            flinkShuffleClient.pushDataHandShake(
                shuffleId, mapId, attemptId, numSubs, bufferSize, partitionLocation);
        // if remainingReviveTimes == 0 and revivePartition.isPresent(), there is no need to send
        // handshake again
        if (revivePartition.isPresent() && remainingReviveTimes > 0) {
          LOG.info(
              "Revive at handshake, currentTimes:{}, totalTimes:{} for shuffleId:{}, mapId:{}, attempId:{}, currentRegionIndex:{}, newPartition:{}, oldPartition:{}",
              remainingReviveTimes,
              maxReviveTimes,
              shuffleId,
              mapId,
              attemptId,
              currentRegionIndex,
              revivePartition,
              partitionLocation);
          partitionLocation = revivePartition.get();
          hasSentHandshake = false;
        } else {
          hasSentHandshake = true;
        }
      }
      if (remainingReviveTimes == 0 && !hasSentHandshake) {
        throw new RuntimeException(
            "After retry " + maxReviveTimes + " times, still failed to send handshake");
      }
    } catch (IOException e) {
      Utils.rethrowAsRuntimeException(e);
    }
  }
}
