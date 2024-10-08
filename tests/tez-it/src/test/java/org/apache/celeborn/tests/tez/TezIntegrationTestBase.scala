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

import java.io.{EOFException, File}
import java.util
import java.util.{ArrayList, HashMap, List, Map, Random}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataInputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.util.{LineReader, Tool, ToolRunner}
import org.apache.tez.client.TezClientUtils
import org.apache.tez.dag.api.TezConfiguration
import org.apache.tez.dag.api.TezConfiguration.TEZ_LIB_URIS
import org.apache.tez.dag.app.CelebornDagAppMaster
import org.apache.tez.test.MiniTezCluster
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.worker.Worker
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils

class TezIntegrationTestBase extends AnyFunSuite with Logging with MiniClusterFeature
  with BeforeAndAfterAll with BeforeAndAfterEach {

  private val TEST_ROOT_DIR =
    "target" + Path.SEPARATOR + classOf[TezIntegrationTestBase].getName + "-tmpDir"

  protected var fs: FileSystem = null
  var conf: Configuration = null
  protected var remoteStagingDir: Path = null
  protected var cluster: MiniDFSCluster = null
  protected var HDFS_URI: String = null
  var miniTezCluster: MiniTezCluster = null
  var workers: collection.Set[Worker] = null
  var master: Master = null

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    val (newMaster, newWorkers) = setupMiniClusterWithRandomPorts()
    master = newMaster
    workers = newWorkers

    // hdfs
    this.conf = new Configuration
    val output = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "output")
    logInfo(s"Output tmp dir: $output")
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, output.getAbsolutePath)
    cluster = new MiniDFSCluster.Builder(conf).build
    HDFS_URI = cluster.getURI.toString + "/"
    fs = new Path(HDFS_URI).getFileSystem(conf)

    // tez
    miniTezCluster = new MiniTezCluster(classOf[TezIntegrationTestBase].getName, 1, 1, 1)
    miniTezCluster.init(conf)
    miniTezCluster.start()
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
    if (miniTezCluster != null) {
      miniTezCluster.stop()
    }
    fs.close()
    cluster.shutdown()
  }

  override def beforeEach(): Unit = {
    remoteStagingDir =
      fs.makeQualified(new Path(TEST_ROOT_DIR, String.valueOf(new Random().nextInt(100000))))
    TezClientUtils.ensureStagingDirExists(conf, remoteStagingDir)
  }

  @throws[Exception]
  def run(): Unit = {
    val appConf = new TezConfiguration(miniTezCluster.getConfig());
    updateCommonConfiguration(appConf);
    runTezApp(appConf, getTestTool, getTestArgs("origin"));
    val originPath = getOutputDir("origin")
    runRssTest("celeborn", originPath)
  }

  @throws[Exception]
  private def runRssTest(testName: String, originPath: String): Unit = {
    val appConf = new TezConfiguration(miniTezCluster.getConfig)
    updateRssConfiguration(appConf)
    appendAndUploadRssJars(appConf)
    runTezApp(appConf, getTestTool, getTestArgs(testName))
    verifyResults(originPath, getOutputDir(testName))
  }

  @throws[Exception]
  protected def runTezApp(tezConf: TezConfiguration, tool: Tool, args: Array[String]): Unit = {
    assertEquals(0, ToolRunner.run(tezConf, tool, args), tool.getClass.getName + " failed")
  }

  protected def appendAndUploadRssJars(tezConf: TezConfiguration): Unit = {
    val uris = tezConf.get(TEZ_LIB_URIS)
    Assertions.assertNotNull(uris)
    // Get the rss client tez shaded jar file.
    val url = classOf[TezIntegrationTestBase].getResource("/")
    val parentPath = new Path(url.getPath).getParent.getParent.getParent.getParent.toString
    val file = new File(parentPath, "client-tez/tez-shaded/target")
    val jars = file.listFiles
    var rssJar: File = null
    for (jar <- jars) {
      if (rssJar == null && jar.getName.startsWith("celeborn-client-tez-shaded")) {
        rssJar = jar
      }
    }
    // upload rss jars
    val testRootDir =
      fs.makeQualified(new Path("target", classOf[TezIntegrationTestBase].getName + "-tmpDir"))
    val appRemoteJar = new Path(testRootDir, "rss-tez-client-shaded.jar")
    fs.copyFromLocalFile(new Path(rssJar.toString), appRemoteJar)
    fs.setPermission(appRemoteJar, new FsPermission("777"))
    // update tez.lib.uris
    tezConf.set(TEZ_LIB_URIS, uris + "," + appRemoteJar)
  }

  def getOutputDir(uniqueOutputName: String): String = {
    Assertions.fail("getOutputDir is not implemented")
    null
  }

  def getTestTool: Tool = null

  def getTestArgs(uniqueOutputName: String) = new Array[String](0)

  def updateCommonConfiguration(appConf: Configuration): Unit = {
    appConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString)
    appConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 512)
    appConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx384m")
    appConf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 512)
    appConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, " -Xmx384m")
  }

  @throws[Exception]
  def updateRssConfiguration(appConf: Configuration): Unit = {
    appConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, remoteStagingDir.toString);
    appConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 512)
    appConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx384m")
    appConf.setInt(TezConfiguration.TEZ_TASK_RESOURCE_MEMORY_MB, 512)
    appConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, " -Xmx384m")
    appConf.set(
      CelebornTezUtils.TEZ_PREFIX + CelebornConf.MASTER_ENDPOINTS.key,
      s"localhost:${master.conf.get(CelebornConf.MASTER_PORT)}")
    appConf.set(
      TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
      TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS_DEFAULT + " " + classOf[CelebornDagAppMaster].getName)
  }

  @throws[Exception]
  def verifyResults(originPath: String, rssPath: String): Unit = {
    verifyResultEqual(originPath, rssPath)
  }

  @throws[Exception]
  def verifyResultEqual(originPath: String, rssPath: String): Unit = {
    if (originPath == null && rssPath == null) return
    val originPathFs: Path = new Path(originPath)
    val rssPathFs: Path = new Path(rssPath)
    val originFiles: Array[FileStatus] = fs.listStatus(originPathFs)
    val rssFiles: Array[FileStatus] = fs.listStatus(rssPathFs)
    var originLen: Long = 0
    var rssLen: Long = 0
    val originFileList: util.List[String] = new util.ArrayList[String]
    val rssFileList: util.List[String] = new util.ArrayList[String]
    for (file <- originFiles) {
      originLen += file.getLen
      val name: String = file.getPath.getName
      if (!(name == "_SUCCESS")) originFileList.add(name)
    }
    for (file <- rssFiles) {
      rssLen += file.getLen
      val name: String = file.getPath.getName
      if (!(name == "_SUCCESS")) rssFileList.add(name)
    }
    assertEquals(originFileList.size, rssFileList.size)
    for (i <- 0 until originFileList.size) {
      assertEquals(originFileList.get(i), rssFileList.get(i))
      val p1: Path = new Path(originPath, originFileList.get(i))
      val f1: FSDataInputStream = fs.open(p1)
      val p2: Path = new Path(rssPath, rssFileList.get(i))
      val f2: FSDataInputStream = fs.open(p2)
      var isNotEof1: Boolean = true
      var isNotEof2: Boolean = true
      while (isNotEof1 && isNotEof2) {
        var b1: Byte = 1
        var b2: Byte = 1
        try b1 = f1.readByte
        catch {
          case ee: EOFException =>
            isNotEof1 = false
        }
        try b2 = f2.readByte
        catch {
          case ee: EOFException =>
            isNotEof2 = false
        }
        assertEquals(b1, b2)
      }
      assertEquals(isNotEof1, isNotEof2)
    }
    assertEquals(originLen, rssLen)
  }

  @throws[Exception]
  def verifyResultsSameSet(originPath: String, rssPath: String): Unit = {
    if (originPath == null && rssPath == null) return
    // 1 List the originalPath and rssPath, make sure generated file are same.
    val originPathFs: Path = new Path(originPath)
    val rssPathFs: Path = new Path(rssPath)
    val originFiles: Array[FileStatus] = fs.listStatus(originPathFs)
    val rssFiles: Array[FileStatus] = fs.listStatus(rssPathFs)
    var originLen: Long = 0
    var rssLen: Long = 0
    val originFileList: util.List[String] = new util.ArrayList[String]
    val rssFileList: util.List[String] = new util.ArrayList[String]
    for (file <- originFiles) {
      originLen += file.getLen
      val name: String = file.getPath.getName
      if (!(name == "_SUCCESS")) originFileList.add(name)
    }
    for (file <- rssFiles) {
      rssLen += file.getLen
      val name: String = file.getPath.getName
      if (!(name == "_SUCCESS")) rssFileList.add(name)
    }
    assertEquals(originFileList.size, rssFileList.size)
    // 2 Load original result and rss result to hashmap
    val originalResults: util.Map[String, Integer] = new util.HashMap[String, Integer]
    import scala.collection.JavaConversions._
    for (file <- originFileList) {
      val path: Path = new Path(originPath, file)
      val lineReader: LineReader = new LineReader(fs.open(path))
      val line: Text = new Text
      while (lineReader.readLine(line) > 0)
        if (!originalResults.containsKey(line.toString)) originalResults.put(line.toString, 1)
        else originalResults.put(line.toString, originalResults.get(line.toString) + 1)
    }
    val rssResults: util.Map[String, Integer] = new util.HashMap[String, Integer]
    import scala.collection.JavaConversions._
    for (file <- rssFileList) {
      val path: Path = new Path(rssPath, file)
      val lineReader: LineReader = new LineReader(fs.open(path))
      val line: Text = new Text
      while (lineReader.readLine(line) > 0)
        if (!rssResults.containsKey(line.toString)) rssResults.put(line.toString, 1)
        else rssResults.put(line.toString, rssResults.get(line.toString) + 1)
    }
    // 3 Compare the hashmap
    Assertions.assertEquals(
      originalResults.size,
      rssResults.size,
      "The size of cartesian product set is not equal")
    import scala.collection.JavaConversions._
    for (entry <- originalResults.entrySet) {
      Assertions.assertTrue(
        rssResults.containsKey(entry.getKey),
        entry.getKey + " is not found in rss cartesian product result")
      Assertions.assertEquals(
        entry.getValue,
        rssResults.get(entry.getKey),
        "the value of " + entry.getKey + " is not equal to in rss cartesian product result")
    }
  }

}
