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
import org.apache.tez.examples.SortMergeJoinExample
import org.junit.jupiter.api.Test

object TezSortMergeJoinTest {
  private val SORT_MERGE_JOIN_OUTPUT_PATH = "sort_merge_join_output"
}
class TezSortMergeJoinTest extends TezJoinIntegrationTestBase {

  test("celeborn tez integration test - Sort Merge Join") {
    fs.delete(new Path(TezSortMergeJoinTest.SORT_MERGE_JOIN_OUTPUT_PATH), true)
    generateInputFile()
    run(TezSortMergeJoinTest.SORT_MERGE_JOIN_OUTPUT_PATH)
  }

  override def getTestTool = new SortMergeJoinExample
  override def getTestArgs(uniqueOutputName: String): Array[String] = Array[String](TezJoinIntegrationTestBase.STREAM_INPUT_PATH, TezJoinIntegrationTestBase.HASH_INPUT_PATH, "2", uniqueOutputName)
  override def getOutputDir(uniqueOutputName: String): String = uniqueOutputName
}
