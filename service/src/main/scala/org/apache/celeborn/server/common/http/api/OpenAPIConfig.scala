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

package org.apache.celeborn.server.common.http.api

import org.glassfish.jersey.server.ResourceConfig

import org.apache.celeborn.server.common.Service

class OpenAPIConfig(serviceName: String) extends ResourceConfig {
  packages(OpenAPIConfig.packages(serviceName): _*)
  register(classOf[CelebornOpenApiResource])
  register(classOf[CelebornScalaObjectMapper])
  register(classOf[RestExceptionMapper])
}

object OpenAPIConfig {
  val packages = Map(
    Service.MASTER -> Seq(
      "org.apache.celeborn.server.common.http.api",
      "org.apache.celeborn.service.deploy.master.http.api"),
    Service.WORKER -> Seq(
      "org.apache.celeborn.server.common.http.api",
      "org.apache.celeborn.service.deploy.worker.http.api"))
}
