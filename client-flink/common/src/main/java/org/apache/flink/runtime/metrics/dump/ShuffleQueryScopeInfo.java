/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics.dump;

import org.apache.flink.runtime.metrics.dump.QueryScopeInfo.TaskQueryScopeInfo;

/**
 * Container for the shuffle scope. Stores the id of the job/vertex, the subtask index and the id of
 * the shuffle.
 */
public class ShuffleQueryScopeInfo extends TaskQueryScopeInfo {

  public final int shuffleId;

  public static final byte INFO_CATEGORY_SHUFFLE = 6;

  public ShuffleQueryScopeInfo(
      String jobID, String vertexID, int subtaskIndex, int attemptNumber, int shuffleId) {
    this(jobID, vertexID, subtaskIndex, attemptNumber, shuffleId, "");
  }

  public ShuffleQueryScopeInfo(
      String jobID,
      String vertexID,
      int subtaskIndex,
      int attemptNumber,
      int shuffleId,
      String scope) {
    super(jobID, vertexID, subtaskIndex, attemptNumber, scope);
    this.shuffleId = shuffleId;
  }

  @Override
  public ShuffleQueryScopeInfo copy(String additionalScope) {
    return new ShuffleQueryScopeInfo(
        this.jobID,
        this.vertexID,
        this.subtaskIndex,
        this.attemptNumber,
        this.shuffleId,
        concatScopes(additionalScope));
  }

  @Override
  public byte getCategory() {
    return INFO_CATEGORY_SHUFFLE;
  }
}
