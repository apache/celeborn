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

package org.apache.celeborn.client.listener;

import java.util.Map;

import org.apache.celeborn.common.protocol.message.StatusCode;

/** The Listener about the partitions revived after partition splits or push failures. */
public interface PartitionReviveListener {

  /**
   * Notifies the partitions that the LifecycleManager moves to new locations. A partition is
   * reported once per revive, however many map tasks requested it.
   *
   * @param shuffleId the Celeborn shuffle id
   * @param appShuffleId the application shuffle id, which differs on stage rerun
   * @param workerRevives revived partitions by worker unique id and revive cause. Replica push
   *     failures count against the replica worker, other causes against the primary worker.
   */
  void onPartitionsRevived(
      int shuffleId, int appShuffleId, Map<String, Map<StatusCode, Integer>> workerRevives);
}
