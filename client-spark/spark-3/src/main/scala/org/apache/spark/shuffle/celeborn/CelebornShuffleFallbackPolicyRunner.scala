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

package org.apache.spark.shuffle.celeborn

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.FallbackPolicy

class CelebornShuffleFallbackPolicyRunner(conf: CelebornConf) extends Logging {
  private val shuffleFallbackPolicy = conf.shuffleFallbackPolicy
  private val checkWorkerEnabled = conf.checkWorkerEnabled

  def applyAllFallbackPolicy(lifecycleManager: LifecycleManager, numPartitions: Int): Boolean = {
    val needFallback =
      applyForceFallbackPolicy() || applyShufflePartitionsFallbackPolicy(numPartitions) ||
        !checkQuota(lifecycleManager) || !checkWorkersAvailable(lifecycleManager)
    if (needFallback && FallbackPolicy.NEVER.equals(shuffleFallbackPolicy)) {
      throw new CelebornIOException(
        "Fallback to spark built-in shuffle implementation is prohibited.")
    }
    needFallback
  }

  /**
   * if celeborn.client.spark.shuffle.fallback.policy is ALWAYS, fallback to spark built-in shuffle implementation
   * @return return true if celeborn.client.spark.shuffle.fallback.policy is ALWAYS, otherwise false
   */
  def applyForceFallbackPolicy(): Boolean = {
    if (FallbackPolicy.ALWAYS.equals(shuffleFallbackPolicy)) {
      logWarning(
        s"${CelebornConf.SPARK_SHUFFLE_FALLBACK_POLICY.key} is ${FallbackPolicy.ALWAYS.name}, " +
          s"forcibly fallback to spark built-in shuffle implementation.")
    }
    FallbackPolicy.ALWAYS.equals(shuffleFallbackPolicy)
  }

  /**
   * if shuffle partitions > celeborn.shuffle.fallback.numPartitionsThreshold, fallback to spark built-in
   * shuffle implementation
   * @param numPartitions shuffle partitions
   * @return return true if shuffle partitions bigger than limit, otherwise false
   */
  def applyShufflePartitionsFallbackPolicy(numPartitions: Int): Boolean = {
    val confNumPartitions = conf.shuffleFallbackPartitionThreshold
    val needFallback = numPartitions >= confNumPartitions
    if (needFallback) {
      logWarning(
        s"Shuffle partition number: $numPartitions exceeds threshold: $confNumPartitions, " +
          "need to fallback to spark built-in shuffle implementation.")
    }
    needFallback
  }

  /**
   * If celeborn cluster is exceed current user's quota, fallback to spark built-in shuffle implementation
   *
   * @return if celeborn cluster have available space for current user
   */
  def checkQuota(lifecycleManager: LifecycleManager): Boolean = {
    if (!conf.quotaEnabled) {
      return true
    }

    val resp = lifecycleManager.checkQuota()
    if (!resp.isAvailable) {
      logWarning(
        s"Quota exceed for current user ${lifecycleManager.getUserIdentifier}. Because: ${resp.reason}")
    }
    resp.isAvailable
  }

  /**
   * If celeborn cluster has no available workers, fallback to spark built-in shuffle implementation
   *
   * @return if celeborn cluster has available workers.
   */
  def checkWorkersAvailable(lifecycleManager: LifecycleManager): Boolean = {
    if (!checkWorkerEnabled) {
      return true
    }

    val resp = lifecycleManager.checkWorkersAvailable()
    if (!resp.getAvailable) {
      logWarning(
        s"No celeborn workers available for current user ${lifecycleManager.getUserIdentifier}.")
    }
    resp.getAvailable
  }
}
