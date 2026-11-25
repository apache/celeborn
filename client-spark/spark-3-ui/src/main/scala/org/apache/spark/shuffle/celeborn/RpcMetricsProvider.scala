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

import scala.util.control.NonFatal

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

import org.apache.celeborn.common.rpc.RpcMetricsSnapshot

/** Resolves the driver's LifecycleManager on demand and caches RPC metrics briefly. */
private[celeborn] object RpcMetricsProvider extends Logging {

  private val cacheTtlMs = 5000L

  private var cached: Option[RpcMetricsSnapshot] = None
  private var cachedAt = 0L
  private var cachedEnv: SparkEnv = _

  def snapshot(): Option[RpcMetricsSnapshot] = synchronized {
    val env = SparkEnv.get
    val now = System.currentTimeMillis()
    if ((env eq cachedEnv) && now - cachedAt < cacheTtlMs) {
      cached
    } else {
      cached = readSnapshot(env)
      cachedEnv = env
      cachedAt = System.currentTimeMillis()
      cached
    }
  }

  private def readSnapshot(env: SparkEnv): Option[RpcMetricsSnapshot] = {
    try {
      Option(env)
        .flatMap(e => Option(e.shuffleManager))
        .collect { case sm: SparkShuffleManager => sm }
        .flatMap(sm => Option(sm.getLifecycleManager()))
        .flatMap(_.rpcMetricsSnapshot())
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to collect LifecycleManager RPC metrics", e)
        None
    }
  }
}
