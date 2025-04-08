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

package org.apache.celeborn.common.write;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.util.JavaUtils;

public class PushState {

  private final int pushBufferMaxSize;
  public AtomicReference<IOException> exception = new AtomicReference<>();
  private final InFlightRequestTracker inFlightRequestTracker;

  private final Map<String, Set<PushFailedBatch>> failedBatchMap;

  public PushState(CelebornConf conf) {
    pushBufferMaxSize = conf.clientPushBufferMaxSize();
    inFlightRequestTracker = new InFlightRequestTracker(conf, this);
    failedBatchMap = new ConcurrentHashMap<>();
  }

  public void cleanup() {
    inFlightRequestTracker.cleanup();
  }

  // key: ${primary addr}, ${replica addr} value: list of data batch
  public final ConcurrentHashMap<Pair<String, String>, DataBatches> batchesMap =
      JavaUtils.newConcurrentHashMap();

  public boolean addBatchData(
      Pair<String, String> addressPair, PartitionLocation loc, int batchId, byte[] body) {
    DataBatches batches = batchesMap.computeIfAbsent(addressPair, (s) -> new DataBatches());
    batches.addDataBatch(loc, batchId, body);
    return batches.getTotalSize() > pushBufferMaxSize;
  }

  public DataBatches takeDataBatches(Pair<String, String> addressPair) {
    return batchesMap.remove(addressPair);
  }

  public int nextBatchId() {
    return inFlightRequestTracker.nextBatchId();
  }

  public void addBatch(int batchId, String hostAndPushPort) {
    inFlightRequestTracker.addBatch(batchId, hostAndPushPort);
  }

  public void removeBatch(int batchId, String hostAndPushPort) {
    inFlightRequestTracker.removeBatch(batchId, hostAndPushPort);
  }

  public void onSuccess(String hostAndPushPort) {
    inFlightRequestTracker.onSuccess(hostAndPushPort);
  }

  public void onCongestControl(String hostAndPushPort) {
    inFlightRequestTracker.onCongestControl(hostAndPushPort);
  }

  public boolean limitMaxInFlight(String hostAndPushPort) throws IOException {
    return inFlightRequestTracker.limitMaxInFlight(hostAndPushPort);
  }

  public boolean limitZeroInFlight() throws IOException {
    return inFlightRequestTracker.limitZeroInFlight();
  }

  public int remainingAllowPushes(String hostAndPushPort) {
    return inFlightRequestTracker.remainingAllowPushes(hostAndPushPort);
  }

  public void addFailedBatch(String partitionId, PushFailedBatch failedBatch) {
    this.failedBatchMap
        .computeIfAbsent(partitionId, (s) -> Sets.newConcurrentHashSet())
        .add(failedBatch);
  }

  public Map<String, Set<PushFailedBatch>> getFailedBatches() {
    return this.failedBatchMap;
  }
}
