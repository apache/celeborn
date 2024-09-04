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

package org.apache.celeborn.tests.tez;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.tez.examples.SortMergeJoinExample;
import org.junit.jupiter.api.Test;

public class TezSortMergeJoinTest extends TezJoinIntegrationTestBase {

  private static final String SORT_MERGE_JOIN_OUTPUT_PATH = "sort_merge_join_output";

  @Test
  public void sortMergeJoinTest() throws Exception {
    fs.delete(new Path(SORT_MERGE_JOIN_OUTPUT_PATH), true);
    generateInputFile();
    run(SORT_MERGE_JOIN_OUTPUT_PATH);
  }

  @Override
  public Tool getTestTool() {
    return new SortMergeJoinExample();
  }

  @Override
  public String[] getTestArgs(String uniqueOutputName) {
    return new String[] {STREAM_INPUT_PATH, HASH_INPUT_PATH, "2", uniqueOutputName};
  }

  @Override
  public String getOutputDir(String uniqueOutputName) {
    return uniqueOutputName;
  }
}
