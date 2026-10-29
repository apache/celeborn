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

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.rpc.RpcMetricsSnapshot

/**
 * Resolves the driver side LifecycleManager on demand and exposes its RPC metrics to the UI.
 *
 * The LifecycleManager is only created when the first shuffle is registered, so it has to be
 * looked up lazily instead of being captured when the plugin initializes. Snapshots are cached
 * for a short while because taking one sorts every histogram reservoir.
 */
private[celeborn] object RpcMetricsProvider extends Logging {

  private val cacheTtlMs = 1000L

  @volatile private var cached: Option[RpcMetricsSnapshot] = None
  @volatile private var cachedAt = 0L
  @volatile private var cachedEnv: SparkEnv = _

  // Accessed only while synchronized on this object. Log once per consecutive failure streak.
  private var failureLogged = false

  def snapshot(): Option[RpcMetricsSnapshot] = {
    val env = SparkEnv.get
    val now = System.currentTimeMillis()
    if (cacheValid(env, now)) {
      cached
    } else {
      synchronized {
        val currentEnv = SparkEnv.get
        val currentTime = System.currentTimeMillis()
        if (cacheValid(currentEnv, currentTime)) {
          cached
        } else {
          if (!(currentEnv eq cachedEnv)) {
            failureLogged = false
          }
          try {
            cached = lifecycleManager(currentEnv).flatMap(_.rpcMetricsSnapshot())
            failureLogged = false
          } catch {
            case NonFatal(e) =>
              cached = None
              if (!failureLogged) {
                logWarning("Failed to collect LifecycleManager RPC metrics", e)
              }
              failureLogged = true
          }
          cachedEnv = currentEnv
          cachedAt = System.currentTimeMillis()
          cached
        }
      }
    }
  }

  private def cacheValid(env: SparkEnv, now: Long): Boolean =
    (env eq cachedEnv) && now - cachedAt < cacheTtlMs

  private def lifecycleManager(env: SparkEnv): Option[LifecycleManager] = {
    if (env == null) {
      return None
    }
    env.shuffleManager match {
      case null => None
      case sm: SparkShuffleManager => Option(sm.getLifecycleManager())
      // Integrations such as Gluten or Auron bring their own ShuffleManager. Use their
      // LifecycleManager accessor when they expose one with the same signature.
      case other =>
        try {
          val method = other.getClass.getMethod("getLifecycleManager")
          if (classOf[LifecycleManager].isAssignableFrom(method.getReturnType)) {
            Option(method.invoke(other).asInstanceOf[LifecycleManager])
          } else {
            None
          }
        } catch {
          case _: NoSuchMethodException => None
        }
    }
  }
}
