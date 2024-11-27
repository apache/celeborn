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

package org.apache.celeborn.plugin.flink.fallback;

import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.common.CelebornConf;

public class WorkersAvailableFallbackPolicy implements ShuffleFallbackPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(WorkersAvailableFallbackPolicy.class);

  public static final WorkersAvailableFallbackPolicy INSTANCE =
      new WorkersAvailableFallbackPolicy();

  /**
   * If celeborn cluster has no available workers, fallback to flink built-in shuffle
   * implementation.
   *
   * @param shuffleContext The job shuffle context of Flink.
   * @param celebornConf The configuration of Celeborn.
   * @param lifecycleManager The {@link LifecycleManager} of Celeborn.
   * @return Whether celeborn cluster has no available workers.
   */
  @Override
  public boolean needFallback(
      JobShuffleContext shuffleContext,
      CelebornConf celebornConf,
      LifecycleManager lifecycleManager) {
    if (!celebornConf.checkWorkerEnabled()) {
      return false;
    }
    boolean needFallback = !lifecycleManager.checkWorkersAvailable().getAvailable();
    if (needFallback) {
      LOG.warn(
          "No celeborn workers available for current user {}.",
          lifecycleManager.getUserIdentifier());
    }
    return needFallback;
  }
}
