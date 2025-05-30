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

package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.runtime.metrics.groups.ShuffleMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;

/** The scope format for the {@link ShuffleMetricGroup}. */
public class ShuffleScopeFormat extends ScopeFormat {

  public static final String SCOPE_SHUFFLE_ID = asVariable("shuffle_id");

  public ShuffleScopeFormat(String format, TaskScopeFormat parentFormat) {
    super(
        format,
        parentFormat,
        new String[] {
          SCOPE_HOST,
          SCOPE_TASKMANAGER_ID,
          SCOPE_JOB_ID,
          SCOPE_JOB_NAME,
          SCOPE_TASK_VERTEX_ID,
          SCOPE_TASK_ATTEMPT_ID,
          SCOPE_TASK_NAME,
          SCOPE_TASK_SUBTASK_INDEX,
          SCOPE_TASK_ATTEMPT_NUM,
          SCOPE_SHUFFLE_ID
        });
  }

  public String[] formatScope(TaskMetricGroup parent, int shuffleId) {
    final String[] template = copyTemplate();
    final String[] values = {
      parent.parent().parent().hostname(),
      parent.parent().parent().taskManagerId(),
      valueOrNull(parent.parent().jobId()),
      valueOrNull(parent.parent().jobName()),
      valueOrNull(parent.vertexId()),
      valueOrNull(parent.executionId()),
      valueOrNull(parent.taskName()),
      String.valueOf(parent.subtaskIndex()),
      String.valueOf(parent.attemptNumber()),
      valueOrNull(shuffleId)
    };
    return bindVariables(template, values);
  }

  // ------------------------------------------------------------------------
  //  Parsing from Config
  // ------------------------------------------------------------------------

  /**
   * Creates the scope format as defined in the given configuration.
   *
   * @param format The scope naming of the format.
   * @param parent The parent {@link TaskScopeFormat} of the format.
   * @return The {@link ShuffleScopeFormat} parsed from the configuration.
   */
  public static ShuffleScopeFormat fromConfig(String format, TaskScopeFormat parent) {
    return new ShuffleScopeFormat(format, parent);
  }
}
