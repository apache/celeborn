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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.celeborn.common.util.JavaUtils;

public class ShuffleTaskInfo {
  private int currentShuffleIndex = 0;
  // task shuffle id -> mapId_taskAttemptId -> attemptIdx
  private ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>
      taskShuffleAttemptIdToAttemptId = JavaUtils.newConcurrentHashMap();
  // map attemptId index
  private ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> taskShuffleAttemptIdIndex =
      JavaUtils.newConcurrentHashMap();
  // task shuffle id -> celeborn shuffle id
  private ConcurrentHashMap<String, Integer> taskShuffleIdToShuffleId =
      JavaUtils.newConcurrentHashMap();
  // celeborn shuffle id -> task shuffle id
  private ConcurrentHashMap<Integer, String> shuffleIdToTaskShuffleId =
      JavaUtils.newConcurrentHashMap();

  public int getShuffleId(String taskShuffleId) {
    synchronized (taskShuffleIdToShuffleId) {
      if (taskShuffleIdToShuffleId.containsKey(taskShuffleId)) {
        return taskShuffleIdToShuffleId.get(taskShuffleId);
      } else {
        taskShuffleIdToShuffleId.put(taskShuffleId, currentShuffleIndex);
        shuffleIdToTaskShuffleId.put(currentShuffleIndex, taskShuffleId);
        int tempShuffleIndex = currentShuffleIndex;
        currentShuffleIndex = currentShuffleIndex + 1;
        return tempShuffleIndex;
      }
    }
  }

  public int getAttemptId(String taskShuffleId, int mapId, String attemptId) {
    ConcurrentHashMap<Integer, Integer> attemptIndex =
        taskShuffleAttemptIdIndex.computeIfAbsent(
            taskShuffleId, (id) -> JavaUtils.newConcurrentHashMap());
    ConcurrentHashMap<String, Integer> attemptIdMap =
        taskShuffleAttemptIdToAttemptId.computeIfAbsent(
            taskShuffleId, (id) -> JavaUtils.newConcurrentHashMap());
    String mapAttemptId = mapId + "_" + attemptId;
    synchronized (attemptIndex) {
      if (!attemptIdMap.containsKey(mapAttemptId)) {
        if (attemptIndex.containsKey(mapId)) {
          int index = attemptIndex.get(mapId);
          attemptIdMap.put(mapAttemptId, index + 1);
          attemptIndex.put(mapId, index + 1);
        } else {
          attemptIdMap.put(mapAttemptId, 0);
          attemptIndex.put(mapId, 0);
        }
      }
    }

    return attemptIdMap.get(mapAttemptId);
  }

  public void removeExpiredShuffle(int shuffleId) {
    if (shuffleIdToTaskShuffleId.containsKey(shuffleId)) {
      String taskShuffleId = shuffleIdToTaskShuffleId.remove(shuffleId);
      taskShuffleIdToShuffleId.remove(taskShuffleId);
      taskShuffleAttemptIdIndex.remove(taskShuffleId);
      taskShuffleAttemptIdToAttemptId.remove(taskShuffleId);
    }
  }
}
