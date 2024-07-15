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

package org.apache.celeborn.common.client

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.RpcNameConstants

abstract class MasterEndpointResolver(
    private val conf: CelebornConf,
    private val isWorker: Boolean) extends Logging {

  private var masterEndpointName: Option[String] = None

  protected var activeMasterEndpoints: Option[List[String]] = None

  protected val updated = new AtomicBoolean(false)

  if (isWorker && conf.internalPortEnabled) {
    // For worker, we should use the internal endpoints if internal port is enabled.
    this.masterEndpointName = Some(RpcNameConstants.MASTER_INTERNAL_EP)
    resolve(conf.masterInternalEndpoints)
  } else {
    this.masterEndpointName = Some(RpcNameConstants.MASTER_EP)
    resolve(conf.masterEndpoints)
  }

  def getMasterEndpointName: String = masterEndpointName.get

  def getActiveMasterEndpoints: java.util.List[String] = activeMasterEndpoints.get.asJava

  def isUpdated: Boolean = updated.compareAndSet(true, false)

  protected def resolve(endpoints: Array[String]): Unit

  protected def update(endpoints: Array[String]): Unit
}
