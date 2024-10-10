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

package org.apache.celeborn.tests.tez

import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Tool
import org.apache.tez.examples.HashJoinExample
import org.junit.jupiter.api.Test

object TezHashJoinTest { private val HASH_JOIN_OUTPUT_PATH = "hash_join_output" }
class TezHashJoinTest extends TezJoinIntegrationTestBase {

  test("celeborn tez integration test - TezHashJoinTest") {
    generateInputFile()
    fs.delete(new Path(TezHashJoinTest.HASH_JOIN_OUTPUT_PATH), true)
    run(TezHashJoinTest.HASH_JOIN_OUTPUT_PATH)
  }

  override def getTestTool = new HashJoinExample
  override def getTestArgs(uniqueOutputName: String): Array[String] = if (uniqueOutputName.contains("broadcast")) Array[String](TezJoinIntegrationTestBase.STREAM_INPUT_PATH, TezJoinIntegrationTestBase.HASH_INPUT_PATH, "2", uniqueOutputName, "doBroadcast") else Array[String](TezJoinIntegrationTestBase.STREAM_INPUT_PATH, TezJoinIntegrationTestBase.HASH_INPUT_PATH, "2", uniqueOutputName)
  override def getOutputDir(uniqueOutputName: String): String = uniqueOutputName
}
