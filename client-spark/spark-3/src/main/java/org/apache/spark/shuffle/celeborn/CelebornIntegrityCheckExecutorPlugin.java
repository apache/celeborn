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

package org.apache.spark.shuffle.celeborn;

import static org.apache.celeborn.client.read.CelebornIntegrityCheckTracker.*;

import org.apache.spark.TaskFailedReason;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark {@link ExecutorPlugin} that manages the per-task lifecycle of
 * {@link org.apache.celeborn.client.read.CelebornIntegrityCheckTracker}.
 *
 * <p>Register this plugin via {@code spark.plugins} to enable client-side enforcement that every
 * {@link org.apache.celeborn.client.read.CelebornInputStream} performs its data integrity check
 * before the task completes.
 *
 * <p>On task start, a fresh per-thread context is initialised. On task success, the context is
 * validated (all registered readers must have finished) and then discarded. On task failure, the
 * context is silently discarded without assertion.
 */
public class CelebornIntegrityCheckExecutorPlugin implements ExecutorPlugin {
  private static final Logger logger =
      LoggerFactory.getLogger(CelebornIntegrityCheckExecutorPlugin.class);

  @Override
  public void onTaskStart() {
    startTask();
  }

  @Override
  public void onTaskSucceeded() {
    finishTask();
  }

  @Override
  public void onTaskFailed(TaskFailedReason failureReason) {
    logger.info("Ignoring integrity check validation as task is failed");
    discard();
  }
}
