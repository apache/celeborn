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

package org.apache.celeborn.client.write;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.StatusCode;

public class PushState {
  class BatchInfo {
    ChannelFuture channelFuture;
    long pushTime = -1;
    RpcResponseCallback callback;
  }

  private static final Logger logger = LoggerFactory.getLogger(PushState.class);

  private int pushBufferMaxSize;
  private long pushTimeout;

  public final AtomicInteger batchId = new AtomicInteger();
  private final ConcurrentHashMap<Integer, BatchInfo> inflightBatchInfos =
      new ConcurrentHashMap<>();
  public AtomicReference<IOException> exception = new AtomicReference<>();

  public PushState(CelebornConf conf) {
    pushBufferMaxSize = conf.pushBufferMaxSize();
    pushTimeout = conf.pushDataTimeoutMs();
  }

  public void addBatch(int batchId) {
    inflightBatchInfos.computeIfAbsent(batchId, id -> new BatchInfo());
  }

  public void removeBatch(int batchId) {
    BatchInfo info = inflightBatchInfos.remove(batchId);
    if (info != null && info.channelFuture != null) {
      info.channelFuture.cancel(true);
    }
  }

  public int inflightBatchCount() {
    return inflightBatchInfos.size();
  }

  public synchronized void failExpiredBatch() {
    long currentTime = System.currentTimeMillis();
    inflightBatchInfos
        .values()
        .forEach(
            info -> {
              if (info.pushTime != -1 && currentTime - info.pushTime > pushTimeout) {
                if (info.callback != null) {
                  info.channelFuture.cancel(true);
                  info.callback.onFailure(
                      new IOException(StatusCode.PUSH_DATA_TIMEOUT.getMessage()));
                  info.channelFuture = null;
                  info.callback = null;
                }
              }
            });
  }

  public void pushStarted(int batchId, ChannelFuture future, RpcResponseCallback callback) {
    BatchInfo info = inflightBatchInfos.get(batchId);
    // In rare cases info could be null. For example, a speculative task has one thread pushing,
    // and other thread retry-pushing. At time 1 thread 1 find StageEnded, then it cleans up
    // PushState, at the same time thread 2 pushes data and calles pushStarted,
    // at this time info will be null
    if (info != null) {
      info.pushTime = System.currentTimeMillis();
      info.channelFuture = future;
      info.callback = callback;
    }
  }

  public void cleanup() {
    if (!inflightBatchInfos.isEmpty()) {
      logger.debug("Cancel all {} futures.", inflightBatchInfos.size());
      inflightBatchInfos
          .values()
          .forEach(
              entry -> {
                if (entry.channelFuture != null) {
                  entry.channelFuture.cancel(true);
                }
              });
      inflightBatchInfos.clear();
    }
  }

  // key: ${master addr}-${slave addr} value: list of data batch
  public final ConcurrentHashMap<String, DataBatches> batchesMap = new ConcurrentHashMap<>();

  /**
   * Not thread-safe
   *
   * @param addressPair
   * @param loc
   * @param batchId
   * @param body
   * @return
   */
  public boolean addBatchData(String addressPair, PartitionLocation loc, int batchId, byte[] body) {
    DataBatches batches = batchesMap.computeIfAbsent(addressPair, (s) -> new DataBatches());
    batches.addDataBatch(loc, batchId, body);
    return batches.getTotalSize() > pushBufferMaxSize;
  }

  public DataBatches takeDataBatches(String addressPair) {
    return batchesMap.remove(addressPair);
  }
}
