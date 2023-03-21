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

package org.apache.celeborn.common.network

import java.io.File
import org.apache.hadoop.fs.CommonConfigurationKeysPublic.{NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY}
import org.apache.hadoop.net.{Node, TableMapping}
import org.apache.hadoop.shaded.com.google.common.base.Charsets
import org.apache.hadoop.shaded.com.google.common.io.Files
import org.junit.Assert.assertEquals

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class CelebornRackResolverSuite extends CelebornFunSuite {

  test("Test TableMapping") {
    val hostName1 = "1.2.3.4"
    val hostName2 = "5.6.7.8"
    val mapFile: File = File.createTempFile(getClass.getSimpleName + ".testResolve", ".txt")
    Files.asCharSink(mapFile, Charsets.UTF_8).write(
      hostName1 + " /rack1\n" + hostName2 + "\t/rack2\n")
    mapFile.deleteOnExit()

    val conf = new CelebornConf
    conf.set(
      "celeborn.hadoop." + NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
      classOf[TableMapping].getName)
    conf.set("celeborn.hadoop." + NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, mapFile.getCanonicalPath)
    val resolver = new CelebornRackResolver(conf)

    val names = Seq(hostName1, hostName2)

    val result: Seq[Node] = resolver.resolve(names)
    assertEquals(names.size, result.size)
    assertEquals("/rack1", result(0).getNetworkLocation)
    assertEquals("/rack2", result(1).getNetworkLocation)
  }
}
