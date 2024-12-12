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

class BroadcastAndOneToOneTest extends TezIntegrationTestBase {

  val outputPath = "broadcast_oneone_output"

  test("celeborn tez integration test - Ordered Word Count") {
    run()
  }

  override def getTestTool = new BroadcastAndOneToOneExample

  override def getTestArgs(uniqueOutputName: String) = new Array[String](0)

  override def getOutputDir(uniqueOutputName: String): String = outputPath + "/" + uniqueOutputName

  override def verifyResults(originPath: String, rssPath: String): Unit = {

  }

}
