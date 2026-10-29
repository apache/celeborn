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

/**
 * Resolves the driver side LifecycleManager on demand and exposes its RPC metrics to the UI.
 *
 * The LifecycleManager is only created when the first shuffle is registered, so it has to be
 * looked up lazily instead of being captured when the plugin initializes.
 */
private[celeborn] object RpcMetricsProvider extends Logging {

  def snapshot(): Option[RpcMetricsSnapshot] = {
    try {
      Option(SparkEnv.get)
        .flatMap(env => Option(env.shuffleManager))
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
