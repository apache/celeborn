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

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.util.Utils

class ContainerInfoSuite extends CelebornFunSuite {

  test("test DefaultContainerInfoProvider") {
    val conf = new CelebornConf
    val defaultContainerInfo = ContainerInfoProvider.instantiate(conf).getContainerInfo()
    assert(defaultContainerInfo.getContainerHostName == Utils.getHostName(false))
    assert(defaultContainerInfo.getContainerAddress == Utils.getHostName(true))
    assert(defaultContainerInfo.getContainerUser == sys.env("user.name"))
  }
}
