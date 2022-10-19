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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.PartitionLocation;

public class PushState {
  private static final Logger logger = LoggerFactory.getLogger(PushState.class);

  private int pushBufferSize;

  public final AtomicInteger batchId = new AtomicInteger();
  public final ConcurrentHashMap<Integer, PartitionLocation> inFlightBatches =
      new ConcurrentHashMap<>();
  public final ConcurrentHashMap<Integer, ChannelFuture> futures = new ConcurrentHashMap<>();
  public AtomicReference<IOException> exception = new AtomicReference<>();

  public PushState(CelebornConf conf) {
    pushBufferSize = CelebornConf.pushBufferMaxSize(conf);
  }

  public void addFuture(int batchId, ChannelFuture future) {
    futures.put(batchId, future);
  }

  public void removeFuture(int batchId) {
    futures.remove(batchId);
  }

  public synchronized void cancelFutures() {
    if (!futures.isEmpty()) {
      Set<Integer> keys = new HashSet<>(futures.keySet());
      logger.debug("Cancel all {} futures.", keys.size());
      for (Integer batchId : keys) {
        ChannelFuture future = futures.remove(batchId);
        if (future != null) {
          future.cancel(true);
        }
      }
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
    return batches.getTotalSize() > pushBufferSize;
  }

  public DataBatches takeDataBatches(String addressPair) {
    return batchesMap.remove(addressPair);
  }
}
