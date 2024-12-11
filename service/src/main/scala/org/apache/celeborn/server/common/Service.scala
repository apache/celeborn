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

package org.apache.celeborn.server.common

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.common.metrics.source.Source
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.server.common.service.config.{ConfigService, DynamicConfigServiceFactory}

abstract class Service extends Logging {
  def serviceName: String

  def conf: CelebornConf

  def metricsSystem: MetricsSystem

  def configService: ConfigService = DynamicConfigServiceFactory.getConfigService(conf)

  def initialize(): Unit = {
    if (conf.metricsSystemEnable) {
      logInfo(s"Metrics system enabled.")
      metricsSystem.start()
    }
  }

  def stop(exitKind: Int): Unit = {}

  // Set the metrics source instance for the service
  serviceName match {
    case Service.MASTER =>
      Source.SOURCE_INSTANCE = s"${Utils.localHostName(conf)}:${conf.masterHttpPort}"
    case Service.WORKER =>
      Source.SOURCE_INSTANCE = s"${Utils.localHostName(conf)}:${conf.workerHttpPort}"
    case _ =>
  }
}

object Service {
  val MASTER = "master"
  val WORKER = "worker"
}
