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

package org.apache.celeborn.service.deploy.web

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils

class WebArgumentsSuite extends AnyFunSuite with Logging {

  test("[CELEBORN-98] Test build webArguments in different case") {
    val args1 = Array.empty[String]
    val conf1 = new CelebornConf()

    val arguments1 = new WebArguments(args1, conf1)
    assert(arguments1.host === sys.env.getOrElse(
      "CELEBORN_LOCAL_HOSTNAME",
      Utils.localHostName(conf1)))
    assert(arguments1.port === 9090)

    // should use celeborn conf
    val conf2 =
      new CelebornConf().set(CelebornConf.WEB_HOST, "test-host-1").set(CelebornConf.WEB_PORT, 19090)

    val arguments2 = new WebArguments(args1, conf2)
    assert(arguments2.host === sys.env.getOrElse("CELEBORN_LOCAL_HOSTNAME", "test-host-1"))
    assert(arguments2.port === 19090)

    // should use cli args
    val arguments3 = new WebArguments(Array("-h", "test-host-1", "-p", "19090"), conf2)
    assert(arguments3.host.equals("test-host-1"))
    assert(arguments3.port == 19090)
  }
}
