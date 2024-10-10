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

import org.apache.commons.lang3.RandomStringUtils
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Tool
import org.apache.tez.examples.CartesianProduct
import org.junit.jupiter.api.Test

class TezCartesianProductTest extends TezIntegrationTestBase {
  private val inputPath1 = "cartesian_product_input1"
  private val inputPath2 = "cartesian_product_input2"
  private val inputPath3 = "cartesian_product_input3"
  private val outputPath = "cartesian_product_output"

  test("celeborn tez integration test - Cartesian Product") {
    generateInputFile()
    run()
  }

  @throws[Exception]
  private def generateInputFile(): Unit = {
    val outputStream1 = fs.create(new Path(inputPath1))
    val outputStream2 = fs.create(new Path(inputPath2))
    val outputStream3 = fs.create(new Path(inputPath3))
    for (i <- 0 until 500) {
      val alphanumeric = RandomStringUtils.randomAlphanumeric(5)
      val numeric = RandomStringUtils.randomNumeric(5)
      outputStream1.writeBytes(alphanumeric + "\n")
      outputStream2.writeBytes(numeric + "\n")
      if (i % 2 == 0) outputStream3.writeBytes(alphanumeric + "\n")
    }
    outputStream1.close()
    outputStream2.close()
    outputStream3.close()
  }
  override def getTestTool = new CartesianProduct
  override def getTestArgs(uniqueOutputName: String): Array[String] = Array[String](
    "-partitioned",
    inputPath1,
    inputPath2,
    inputPath3,
    outputPath + "/" + uniqueOutputName)
  override def getOutputDir(uniqueOutputName: String): String = outputPath + "/" + uniqueOutputName
  @throws[Exception]
  override def verifyResults(originPath: String, rssPath: String): Unit = {
    verifyResultsSameSet(originPath, rssPath)
  }
}
