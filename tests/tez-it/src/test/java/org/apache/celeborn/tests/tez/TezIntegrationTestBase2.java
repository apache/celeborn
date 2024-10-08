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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.CelebornDagAppMaster;
import org.apache.tez.test.MiniTezCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;

public class TezIntegrationTestBase2 {

  private static final Logger LOG = LoggerFactory.getLogger(TezIntegrationTestBase.class);
  protected static MiniTezCluster miniTezCluster;

  protected static FileSystem fs;

  @BeforeAll
  public static void beforeClass() throws Exception {
    miniTezCluster = new MiniTezCluster(TezIntegrationTestBase.class.getName(), 1, 1, 1);
    Configuration conf = new Configuration();
    miniTezCluster.init(conf);
    miniTezCluster.start();
    fs = FileSystem.get(conf);
  }

  public void run() throws Exception {

    // TezConfiguration appConf = new TezConfiguration(miniTezCluster.getConfig());
    TezConfiguration appConf = new TezConfiguration();
    updateCommonConfiguration(appConf);
    // runTezApp(appConf, getTestTool(), getTestArgs("test"));

    appConf = new TezConfiguration();
    updateRssConfiguration(appConf);
    runTezApp(appConf, getTestTool(), getTestArgs("rss"));
  }

  protected void runTezApp(TezConfiguration tezConf, Tool tool, String[] args) throws Exception {
    ToolRunner.run(tezConf, tool, args);
  }

  public void updateCommonConfiguration(Configuration appConf) throws Exception {
    // appConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
    appConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 512);
    appConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx384m");
    appConf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 512);
    appConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, " -Xmx384m");
  }

  public void updateRssConfiguration(Configuration appConf) throws Exception {
    // appConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString());
    appConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 512);
    appConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx384m");
    appConf.set(
        TezConfiguration.TEZ_PREFIX + "appmaster.class.name",
        "org.apache.tez.dag.app.CelebornDagAppMaster");
    appConf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 512);
    appConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, " -Xmx384m");
    appConf.set(
        CelebornTezUtils.TEZ_PREFIX + CelebornConf.MASTER_ENDPOINTS().key(), "localhost:9097");
    appConf.set(
        TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
        TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS_DEFAULT
            + " -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 "
            + CelebornDagAppMaster.class.getName());
  }

  public Tool getTestTool() {
    return null;
  }

  public String[] getTestArgs(String uniqueOutputName) {
    return new String[0];
  }

  public String getOutputDir(String uniqueOutputName) {
    return null;
  }

  public void verifyResults(String originPath, String rssPath) throws Exception {
    verifyResultEqual(originPath, rssPath);
  }

  public static void verifyResultEqual(String originPath, String rssPath) throws Exception {
    if (originPath == null && rssPath == null) {
      return;
    }
    Path originPathFs = new Path(originPath);
    Path rssPathFs = new Path(rssPath);
    FileStatus[] originFiles = fs.listStatus(originPathFs);
    FileStatus[] rssFiles = fs.listStatus(rssPathFs);
    long originLen = 0;
    long rssLen = 0;
    List<String> originFileList = new ArrayList<>();
    List<String> rssFileList = new ArrayList<>();
    for (FileStatus file : originFiles) {
      originLen += file.getLen();
      String name = file.getPath().getName();
      if (!name.equals("_SUCCESS")) {
        originFileList.add(name);
      }
    }
    for (FileStatus file : rssFiles) {
      rssLen += file.getLen();
      String name = file.getPath().getName();
      if (!name.equals("_SUCCESS")) {
        rssFileList.add(name);
      }
    }
    assertEquals(originFileList.size(), rssFileList.size());
    for (int i = 0; i < originFileList.size(); i++) {
      assertEquals(originFileList.get(i), rssFileList.get(i));
      Path p1 = new Path(originPath, originFileList.get(i));
      FSDataInputStream f1 = fs.open(p1);
      Path p2 = new Path(rssPath, rssFileList.get(i));
      FSDataInputStream f2 = fs.open(p2);
      boolean isNotEof1 = true;
      boolean isNotEof2 = true;
      while (isNotEof1 && isNotEof2) {
        byte b1 = 1;
        byte b2 = 1;
        try {
          b1 = f1.readByte();
        } catch (EOFException ee) {
          isNotEof1 = false;
        }
        try {
          b2 = f2.readByte();
        } catch (EOFException ee) {
          isNotEof2 = false;
        }
        assertEquals(b1, b2);
      }
      assertEquals(isNotEof1, isNotEof2);
    }
    assertEquals(originLen, rssLen);
  }

  public static void verifyResultsSameSet(String originPath, String rssPath) throws Exception {
    if (originPath == null && rssPath == null) {
      return;
    }
    // 1 List the originalPath and rssPath, make sure generated file are same.
    Path originPathFs = new Path(originPath);
    Path rssPathFs = new Path(rssPath);
    FileStatus[] originFiles = fs.listStatus(originPathFs);
    FileStatus[] rssFiles = fs.listStatus(rssPathFs);
    long originLen = 0;
    long rssLen = 0;
    List<String> originFileList = new ArrayList<>();
    List<String> rssFileList = new ArrayList<>();
    for (FileStatus file : originFiles) {
      originLen += file.getLen();
      String name = file.getPath().getName();
      if (!name.equals("_SUCCESS")) {
        originFileList.add(name);
      }
    }
    for (FileStatus file : rssFiles) {
      rssLen += file.getLen();
      String name = file.getPath().getName();
      if (!name.equals("_SUCCESS")) {
        rssFileList.add(name);
      }
    }
    assertEquals(originFileList.size(), rssFileList.size());

    // 2 Load original result and rss result to hashmap
    Map<String, Integer> originalResults = new HashMap<>();
    for (String file : originFileList) {
      Path path = new Path(originPath, file);
      LineReader lineReader = new LineReader(fs.open(path));
      Text line = new Text();
      while (lineReader.readLine(line) > 0) {
        if (!originalResults.containsKey(line.toString())) {
          originalResults.put(line.toString(), 1);
        } else {
          originalResults.put(line.toString(), originalResults.get(line.toString()) + 1);
        }
      }
    }

    Map<String, Integer> rssResults = new HashMap<>();
    for (String file : rssFileList) {
      Path path = new Path(rssPath, file);
      LineReader lineReader = new LineReader(fs.open(path));
      Text line = new Text();
      while (lineReader.readLine(line) > 0) {
        if (!rssResults.containsKey(line.toString())) {
          rssResults.put(line.toString(), 1);
        } else {
          rssResults.put(line.toString(), rssResults.get(line.toString()) + 1);
        }
      }
    }

    // 3 Compare the hashmap
    Assertions.assertEquals(
        originalResults.size(),
        rssResults.size(),
        "The size of cartesian product set is not equal");
    for (Map.Entry<String, Integer> entry : originalResults.entrySet()) {
      Assertions.assertTrue(
          rssResults.containsKey(entry.getKey()),
          entry.getKey() + " is not found in rss cartesian product result");
      Assertions.assertEquals(
          entry.getValue(),
          rssResults.get(entry.getKey()),
          "the value of " + entry.getKey() + " is not equal to in rss cartesian product result");
    }
  }
}
