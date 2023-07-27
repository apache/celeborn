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

package org.apache.celeborn.server.common.http

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.internal.Logging

class HttpUtilsSuite extends AnyFunSuite with Logging {

  def checkParseUri(
      uri: String,
      expectPath: String,
      expectParameters: Map[String, String]): Unit = {
    val (path, parameters) = HttpUtils.parseUrl(uri)
    assert(path == expectPath)
    assert(parameters == expectParameters)
  }

  test("CELEBORN-847: Support parse HTTP Restful API parameters") {
    checkParseUri("/exit", "/exit", Map.empty)
    checkParseUri("/exit?type=decommission", "/exit", Map("TYPE" -> "DECOMMISSION"))
    checkParseUri(
      "/exit?type=decommission&foo=a",
      "/exit",
      Map("TYPE" -> "DECOMMISSION", "FOO" -> "A"))
  }
}
