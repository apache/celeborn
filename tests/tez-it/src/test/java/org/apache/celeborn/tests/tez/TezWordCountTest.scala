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

import java.io.FileOutputStream
import java.nio.charset.StandardCharsets
import java.util.Random

import com.google.common.collect.Lists
import org.apache.hadoop.fs.Path
import org.apache.tez.examples.WordCount

class TezWordCountTest extends TezIntegrationTestBase {
  private val inputPath = "word_count_input"
  private val outputPath = "word_count_output"
  private val wordTable = Lists.newArrayList(
    "apple",
    "banana",
    "fruit",
    "cherry",
    "Chinese",
    "America",
    "Japan",
    "tomato")

  test("celeborn tez integration test - Word Count") {
    generateInputFile()
    run()
  }

  @throws[Exception]
  private def generateInputFile(): Unit = {
    val outputStream = fs.create(new Path(inputPath))
    val random = new Random
    for (i <- 0 until 100) {
      val index = random.nextInt(wordTable.size)
      val str = wordTable.get(index) + "\n"
      outputStream.write(str.getBytes(StandardCharsets.UTF_8))
    }
    outputStream.close()
  }
  override def getTestTool = new WordCount
  override def getTestArgs(uniqueOutputName: String): Array[String] =
    Array[String](inputPath, outputPath + "/" + uniqueOutputName, "2")
  override def getOutputDir(uniqueOutputName: String): String = outputPath + "/" + uniqueOutputName
}
