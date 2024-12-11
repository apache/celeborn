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

package org.apache.celeborn.common.rpc

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.{AbstractSource, Role}

class RpcSource(conf: CelebornConf) extends AbstractSource(conf, Role.RPC) {
  import RpcSource._

  override def sourceName: String = Role.RPC

  startCleaner()

  def queueLengthMetric(name: String): String = {
    metricNameWithCustomizedLabels(QUEUE_LENGTH, Map(NAME_LABEL -> name))
  }

  def queueTimeMetric(name: String): String = {
    metricNameWithCustomizedLabels(QUEUE_TIME, Map(NAME_LABEL -> name))
  }

  def processTimeMetric(name: String): String = {
    metricNameWithCustomizedLabels(PROCESS_TIME, Map(NAME_LABEL -> name))
  }
}

object RpcSource {
  private val QUEUE_LENGTH = "RpcQueueLength"
  private val QUEUE_TIME = "RpcQueueTime"
  private val PROCESS_TIME = "RpcProcessTime"

  private val NAME_LABEL = "name"
}
