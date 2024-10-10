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

import java.util._
import com.google.common.collect.Lists
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Tool
import org.apache.tez.examples.OrderedWordCount
import org.junit.jupiter.api.Test

import java.util

class TezOrderedWordCountTest extends TezIntegrationTestBase {
  private val inputPath = "ordered_word_count_input"
  private val outputPath = "ordered_word_count_output"
  private val wordTable = Lists.newArrayList(
    "apple",
    "banana",
    "fruit",
    "cherry",
    "Chinese",
    "America",
    "Japan",
    "tomato")

  test("celeborn tez integration test - Ordered Word Count") {
    generateInputFile()
    run()
  }

  @throws[Exception]
  private def generateInputFile(): Unit = {
    // For ordered word count, the key of last ordered sorter is the summation of word, the value is
    // the word. So it means this key may not be unique. Because Sorter can only make sure key is
    // sorted, so the second column (word column) may be not sorted.
    // To keep pace with verifyResults, here make sure summation of word is unique number.
    val outputStream = fs.create(new Path(inputPath))
    val random = new Random
    val used = new util.HashSet[Int]()
    val outputList = new util.ArrayList[String]
    var index = 0
    while (index < wordTable.size) {
      val summation = random.nextInt(50)
      if (!used.contains(summation)) {
        used.add(summation)
        for (i <- 0 until summation) { outputList.add(wordTable.get(index)) }
        index += 1
      }

    }
    Collections.shuffle(outputList)
    import scala.collection.JavaConversions._
    for (word <- outputList) { outputStream.writeBytes(word + "\n") }
    outputStream.close()
  }
  override def getTestTool = new OrderedWordCount
  override def getTestArgs(uniqueOutputName: String): Array[String] = Array[String](inputPath, outputPath + "/" + uniqueOutputName, "2")
  override def getOutputDir(uniqueOutputName: String): String = outputPath + "/" + uniqueOutputName
}
