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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.examples.JoinDataGen;
import org.apache.tez.examples.JoinValidate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TezJoinIntegrationTestBase extends TezIntegrationTestBase {

  protected static final String STREAM_INPUT_PATH = "stream_input";
  protected static final String STREAM_INPUT_FILE_SIZE = "5000000";
  protected static final String HASH_INPUT_PATH = "hash_input";
  protected static final String HASH_INPUT_FILE_SIZE = "500000";
  protected static final String JOIN_EXPECTED_PATH = "join_expected";
  protected static final String NUM_TASKS = "2";

  protected void generateInputFile() throws Exception {

    fs.delete(new Path(STREAM_INPUT_PATH), true);
    fs.delete(new Path(HASH_INPUT_PATH), true);
    fs.delete(new Path(JOIN_EXPECTED_PATH), true);
    String[] args = {
      STREAM_INPUT_PATH,
      STREAM_INPUT_FILE_SIZE,
      HASH_INPUT_PATH,
      HASH_INPUT_FILE_SIZE,
      JOIN_EXPECTED_PATH,
      NUM_TASKS
    };
    JoinDataGen dataGen = new JoinDataGen();
    TezConfiguration appConf = new TezConfiguration(miniTezCluster.getConfig());
    updateCommonConfiguration(appConf);
    assertEquals(0, ToolRunner.run(appConf, dataGen, args), "JoinDataGen failed");
  }

  public void run(String path) throws Exception {
    // TezConfiguration appConf = new TezConfiguration(miniTezCluster.getConfig());
    TezConfiguration appConf = new TezConfiguration();
    //updateCommonConfiguration(appConf);
    //runTezApp(appConf, getTestTool(), getTestArgs("test"));

    updateRssConfiguration(appConf);
    runTezApp(appConf, getTestTool(), getTestArgs(path));
    verifyResults(JOIN_EXPECTED_PATH, getOutputDir(path));
  }

  public void verifyResults(String expectedPath, String rssPath) throws Exception {
    String[] args = {expectedPath, rssPath};
    JoinValidate validate = new JoinValidate();
    TezConfiguration appConf = new TezConfiguration(miniTezCluster.getConfig());
    updateCommonConfiguration(appConf);
    assertEquals(0, ToolRunner.run(appConf, validate, args), "JoinValidate failed");
  }
}
