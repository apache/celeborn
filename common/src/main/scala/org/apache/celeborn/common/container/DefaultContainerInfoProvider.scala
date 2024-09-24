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

package org.apache.celeborn.common.container

import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.rest.v1.model.ContainerInfo

class DefaultContainerInfoProvider extends ContainerInfoProvider {

  override def getContainerInfo(): ContainerInfo = {
    new ContainerInfo()
      .containerName(ContainerInfoProvider.DEFAULT_CONTAINER_NAME)
      .containerHostName(Utils.getHostName(false))
      .containerAddress(Utils.getHostName(true))
      .containerDataCenter(ContainerInfoProvider.DEFAULT_CONTAINER_DATA_CENTER)
      .containerAvailabilityZone(ContainerInfoProvider.DEFAULT_CONTAINER_AVAILABILITY_ZONE)
      .containerUser(sys.env("user.name"))
      .containerCluster(ContainerInfoProvider.DEFAULT_CONTAINER_CLUSTER)
      .containerTags(ContainerInfoProvider.DEFAULT_CONTAINER_TAGS)
  }
}
