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

import scala.util.Try

import org.apache.spark.SparkEnv

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.rpc.RpcMetricsSnapshot

/**
 * Resolves the driver side LifecycleManager on demand and exposes its RPC metrics to the UI.
 *
 * The LifecycleManager is only created when the first shuffle is registered, so it has to be
 * looked up lazily instead of being captured when the plugin initializes. Snapshots are cached
 * for a short while because taking one sorts every histogram reservoir.
 */
private[celeborn] object RpcMetricsProvider {

  private val cacheTtlMs = 1000L

  @volatile private var cached: Option[RpcMetricsSnapshot] = None
  @volatile private var cachedAt = 0L

  def snapshot(): Option[RpcMetricsSnapshot] = {
    val now = System.currentTimeMillis()
    if (now - cachedAt < cacheTtlMs) {
      cached
    } else {
      synchronized {
        if (now - cachedAt < cacheTtlMs) {
          cached
        } else {
          cached = Try(lifecycleManager().flatMap(_.rpcMetricsSnapshot())).getOrElse(None)
          cachedAt = System.currentTimeMillis()
          cached
        }
      }
    }
  }

  private def lifecycleManager(): Option[LifecycleManager] = {
    val env = SparkEnv.get
    if (env == null) {
      return None
    }
    env.shuffleManager match {
      case sm: SparkShuffleManager => Option(sm.getLifecycleManager())
      // Integrations such as Gluten or Auron bring their own ShuffleManager. Use their
      // LifecycleManager accessor when they expose one with the same signature.
      case other =>
        Try {
          val method = other.getClass.getMethod("getLifecycleManager")
          if (classOf[LifecycleManager].isAssignableFrom(method.getReturnType)) {
            Option(method.invoke(other).asInstanceOf[LifecycleManager])
          } else {
            None
          }
        }.getOrElse(None)
    }
  }
}
