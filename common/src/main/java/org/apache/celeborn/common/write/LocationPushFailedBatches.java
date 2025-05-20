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

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.Utils;

public class LocationPushFailedBatches implements Serializable {

  // map-attempt-id -> failed batch ids
  private final Map<String, Set<Integer>> failedBatches = JavaUtils.newConcurrentHashMap();

  public Map<String, Set<Integer>> getFailedBatches() {
    return failedBatches;
  }

  public boolean contains(int mapId, int attemptId, int batchId) {
    Set<Integer> batches = failedBatches.get(Utils.makeAttemptKey(mapId, attemptId));
    return batches != null && batches.contains(batchId);
  }

  public void merge(LocationPushFailedBatches batches) {
    Map<String, Set<Integer>> otherFailedBatchesMap = batches.getFailedBatches();
    otherFailedBatchesMap.forEach(
        (k, v) -> {
          Set<Integer> failedBatches =
              this.failedBatches.computeIfAbsent(k, (s) -> ConcurrentHashMap.newKeySet());
          failedBatches.addAll(v);
        });
  }

  public void addFailedBatch(int mapId, int attemptId, int batchId) {
    String attemptKey = Utils.makeAttemptKey(mapId, attemptId);
    Set<Integer> failedBatches =
        this.failedBatches.computeIfAbsent(attemptKey, (s) -> ConcurrentHashMap.newKeySet());
    failedBatches.add(batchId);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    LocationPushFailedBatches that = (LocationPushFailedBatches) o;
    if (that.failedBatches.size() != failedBatches.size()) {
      return false;
    }

    return failedBatches.entrySet().stream()
        .allMatch(
            item -> {
              Set<Integer> failedBatchesSet = that.failedBatches.get(item.getKey());
              if (failedBatchesSet == null) return false;
              return failedBatchesSet.equals(item.getValue());
            });
  }

  @Override
  public int hashCode() {
    return failedBatches.entrySet().hashCode();
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    failedBatches.forEach(
        (attemptKey, value) -> {
          stringBuilder.append("failed attemptKey:");
          stringBuilder.append(attemptKey);
          stringBuilder.append(" fail batch Ids:");
          stringBuilder.append(StringUtils.join(value, ","));
        });
    return "LocationPushFailedBatches{" + "failedBatches=" + stringBuilder + '}';
  }
}
