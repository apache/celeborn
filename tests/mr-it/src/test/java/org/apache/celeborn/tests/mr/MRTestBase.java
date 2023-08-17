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

package org.apache.celeborn.tests.mr;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.mapreduce.v2.TestMRJobs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.service.deploy.master.Master;
import org.apache.celeborn.service.deploy.master.MasterArguments;
import org.apache.celeborn.service.deploy.worker.Worker;
import org.apache.celeborn.service.deploy.worker.WorkerArguments;

public class MRTestBase {
  private static Logger logger = LoggerFactory.getLogger(MRTestBase.class);
  protected static Configuration conf;
  protected static String HDFS_URI;
  protected static FileSystem fs;
  protected static MiniDFSCluster dfsCluster;
  protected static File baseDir;
  protected static MiniYARNCluster yarnCluster;
  protected static FileSystem localFS;
  protected static Path testDir;
  protected static Path appJar;
  protected static String outputDir;
  protected static Path testResourceDir;
  protected static File celebornBase;
  protected static Master master;
  protected static Worker worker1;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    conf = new Configuration();
    File tmpDir = Files.createTempDir();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpDir.getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(conf).build();
    HDFS_URI = dfsCluster.getURI().toString() + "/";
    fs = new Path(HDFS_URI).getFileSystem(conf);

    localFS = FileSystem.getLocal(conf);
    testDir = localFS.makeQualified(new Path("target", TestMRJobs.class.getName() + "-tmpDir"));
    appJar = new Path(testDir, "MRAppJar.jar");
    outputDir = "/tmp/" + TestMRJobs.class.getName();
    testResourceDir = new Path(testDir, "local");
    yarnCluster = new MiniMRYarnCluster("test");
    conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/app-staging");
    yarnCluster.init(conf);
    yarnCluster.start();

    localFS.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), appJar);
    localFS.setPermission(appJar, new FsPermission("0755"));

    celebornBase = Files.createTempDir();
    celebornBase.deleteOnExit();

    CelebornConf conf = new CelebornConf();
    MasterArguments masterArgs = new MasterArguments(new String[0], conf);
    master = new Master(conf, masterArgs);
    master.initialize();

    worker1 = initlizedWorker(0);
    // wait for worker to register with master
    Thread.sleep(10_000l);
  }

  private static Worker initlizedWorker(int index) {
    CelebornConf conf = new CelebornConf();
    File workerBaseDir = new File(celebornBase, "worker-" + index);
    workerBaseDir.mkdirs();
    conf.set("celeborn.worker.dirs", workerBaseDir.getPath());
    WorkerArguments workerArgument2 = new WorkerArguments(new String[0], conf);
    Worker worker = new Worker(conf, workerArgument2);
    worker.initialize();
    return worker;
  }

  @AfterClass
  public static void shutdown() throws IOException {
    fs.close();
    dfsCluster.shutdown();
    yarnCluster.stop();
    worker1.shutdown();
  }

  public Tool getTestCaseClass() {
    return null;
  }

  public void testMRApp() throws Exception {
    String[] args = new String[0];
    JobConf appConf = new JobConf(yarnCluster.getConfig());
    appConf.setInt(MRJobConfig.MAP_MEMORY_MB, 256);
    logger.info("Run wordcount without celeborn");
    ToolRunner.run(appConf, getTestCaseClass(), args);

    final String shuffleResultPath = appConf.get("mapreduce.output.fileoutputformat.outputdir");
    appConf = new JobConf(yarnCluster.getConfig());
    appConf.setInt(MRJobConfig.MAP_MEMORY_MB, 256);
    addCelebornConfs(appConf);
    ToolRunner.run(appConf, getTestCaseClass(), args);
    String celebornShuffleResultPath = appConf.get("mapreduce.output.fileoutputformat.outputdir");
    compareNativeShuffleAndCelebornShuffleResult(shuffleResultPath, celebornShuffleResultPath);
  }

  private void compareNativeShuffleAndCelebornShuffleResult(
      String nativeShufflePathStr, String shuffleWithCelebornPathStr) throws IOException {
    Path nativeShufflePath = new Path(nativeShufflePathStr);
    Path celebornShufflePath = new Path(shuffleWithCelebornPathStr);
    FileStatus[] shuffleFiles = fs.listStatus(nativeShufflePath);
    FileStatus[] celebornShuffleFiles = fs.listStatus(celebornShufflePath);

    long nativeShuffleSize = 0;
    long celebornShuffleSize = 0;
    List<String> shuffleFileList = new ArrayList<>();
    List<String> celebornFileList = new ArrayList<>();
    for (FileStatus shuffleFile : shuffleFiles) {
      nativeShuffleSize += shuffleFile.getLen();
      String name = shuffleFile.getPath().getName();
      if (!name.equals("_SUCCESS")) {
        shuffleFileList.add(name);
      }
    }
    for (FileStatus celebornShuffleFile : celebornShuffleFiles) {
      celebornShuffleSize += celebornShuffleFile.getLen();
      String name = celebornShuffleFile.getPath().getName();
      if (!name.equals("_SUCCESS")) {
        celebornFileList.add(name);
      }
    }

    Assert.assertEquals(nativeShuffleSize, celebornShuffleSize);
    Assert.assertEquals(shuffleFileList.size(), celebornFileList.size());

    for (int i = 0; i < shuffleFileList.size(); i++) {
      Assert.assertEquals(shuffleFileList.get(i), celebornFileList.get(i));
      Path nativeShuffleFilePath = new Path(nativeShufflePath, shuffleFileList.get(i));
      Path celebornShuffleFilePath = new Path(celebornShufflePath, celebornFileList.get(i));
      FSDataInputStream nativeShuffleFileStream = fs.open(nativeShuffleFilePath);
      FSDataInputStream celebornShuffleFileStream = fs.open(celebornShuffleFilePath);
      byte b1 = 0;
      byte b2 = 0;
      boolean notEof1 = true;
      boolean notEof2 = true;
      while (notEof1 && notEof2) {
        try {
          b1 = (byte) nativeShuffleFileStream.read();
        } catch (EOFException e1) {
          notEof1 = false;
        }
        try {
          b2 = (byte) celebornShuffleFileStream.read();
        } catch (EOFException e2) {
          notEof2 = false;
        }
        Assert.assertEquals(b1, b2);
      }
      Assert.assertEquals(notEof1, notEof2);
    }
  }

  private void addCelebornConfs(JobConf jobConf) throws IOException {
    URL url = MRTestBase.class.getResource("/");
    String itBase =
        new Path(url.getPath()).getParent().getParent().getParent().getParent().toString();
    jobConf.set(
        MRJobConfig.MR_AM_COMMAND_OPTS,
        "-XX:+TraceClassLoading org.apache.celeborn.mapreduce.v2.app.MRAppMasterWithCeleborn");
    jobConf.set(
        MRJobConfig.REDUCE_JAVA_OPTS, "-XX:+TraceClassLoading -XX:MaxDirectMemorySize=524288000");
    jobConf.setInt(MRJobConfig.MAP_MEMORY_MB, 600);
    jobConf.setInt(MRJobConfig.REDUCE_MEMORY_MB, 1024);
    jobConf.setInt(MRJobConfig.IO_SORT_FACTOR, 256);
    jobConf.set(
        MRJobConfig.MAP_OUTPUT_COLLECTOR_CLASS_ATTR,
        "org.apache.hadoop.mapred.CelebornMapOutputCollector");
    jobConf.set(
        MRConfig.SHUFFLE_CONSUMER_PLUGIN,
        "org.apache.hadoop.mapreduce.task.reduce.CelebornShuffleConsumer");
    jobConf.set("mapreduce.celeborn.master.endpoints", "localhost:9097");

    File shadeMrClientDir = new File(itBase, "client-mr/mr-shade/target/");
    File[] jars = shadeMrClientDir.listFiles();
    File clientJar = null;
    for (File jar : jars) {
      if (jar.getName().startsWith("celeborn-client-mr") && !jar.getName().contains("tests")) {
        clientJar = jar;
        break;
      }
    }

    String props = System.getProperty("java.class.path");
    String newProps = "";
    String[] splittedProps = props.split(":");
    for (String prop : splittedProps) {
      if (!prop.contains("classes") && !prop.contains("grpc") && !prop.contains("celeborn-")) {
        newProps = newProps + ":" + prop;
      } else if (prop.contains("mr")) {
        newProps = newProps + ":" + prop;
      }
    }
    System.setProperty("java.class.path", newProps);

    Path newPath = new Path(HDFS_URI + "/celeborn");
    FileUtil.copy(clientJar, fs, newPath, false, jobConf);
    DistributedCache.addFileToClassPath(new Path(newPath.toUri().getPath()), jobConf, fs);

    jobConf.set(
        MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
        "$PWD/celeborn/"
            + clientJar.getName()
            + ","
            + MRJobConfig.DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH);
  }
}
