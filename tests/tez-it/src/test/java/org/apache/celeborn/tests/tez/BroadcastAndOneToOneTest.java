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

import org.apache.hadoop.util.Tool;
import org.junit.Test;

public class BroadcastAndOneToOneTest extends TezIntegrationTestBase {
  public static final String outputPath = "broadcast_oneone_output";

  @Test
  public void hashJoinTest() throws Exception {
    MiniCelebornCluster.setupMiniClusterWithRandomPorts();
    run();
  }

  @Override
  public Tool getTestTool() {
    return new BroadcastAndOneToOneExample();
  }

  @Override
  public String[] getTestArgs(String uniqueOutputName) {
    return new String[] {};
  }

  @Override
  public String getOutputDir(String uniqueOutputName) {
    return outputPath + "/" + uniqueOutputName;
  }
}
