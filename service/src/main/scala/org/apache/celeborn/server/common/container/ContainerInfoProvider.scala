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

package org.apache.celeborn.server.common.container

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.rest.v1.model.ContainerInfo

abstract class ContainerInfoProvider {

  def getContainerInfo(): ContainerInfo

}

object ContainerInfoProvider extends Logging {

  val DEFAULT_CONTAINER_NAME = "default_container_name"
  val DEFAULT_CONTAINER_DATA_CENTER = "default_container_data_center"
  val DEFAULT_CONTAINER_AVAILABILITY_ZONE = "default_container_availability_zone"
  val DEFAULT_CONTAINER_CLUSTER = "default_container_cluster"
  val DEFAULT_CONTAINER_TAGS = List.empty[String].asJava

  def instantiate(conf: CelebornConf): ContainerInfoProvider = {
    Utils.instantiate(conf.containerInfoProviderClass)
  }

}
