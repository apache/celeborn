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

package org.apache.celeborn.service.deploy.master.scale

import java.util.concurrent.TimeUnit

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{ThreadUtils, Utils}
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager

abstract class AbstractScaleManager(protected val conf: CelebornConf)
  extends ScaleManager with Logging {

  protected var statusSystem: AbstractMetaManager = _
  private val scaleService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("celeborn-scale-service")

  override def init(metaManager: AbstractMetaManager): Unit = {
    statusSystem = metaManager
  }

  override def run(): Unit = {
    val interval = conf.get(CelebornConf.SCALE_CHECK_INTERVAL)
    scaleService.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError(checkScale())
      },
      interval,
      interval,
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = ThreadUtils.shutdown(scaleService)

  protected def checkScale(): Unit

}
