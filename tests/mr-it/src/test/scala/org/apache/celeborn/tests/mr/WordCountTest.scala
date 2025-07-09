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

package org.apache.celeborn.tests.mr

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.examples.WordCount
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.ShuffleHandler
import org.apache.hadoop.mapreduce.{Job, MRJobConfig}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.service.Service
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.worker.Worker

class WordCountTest extends AnyFunSuite with Logging with MiniClusterFeature
  with BeforeAndAfterAll {
  var workers: collection.Set[Worker] = null
  var master: Master = null

  var yarnCluster: MiniYARNCluster = null
  var hadoopConf: Configuration = null

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    val (newMaster, newWorkers) = setupMiniClusterWithRandomPorts()
    master = newMaster
    workers = newWorkers

    hadoopConf = new Configuration()
    hadoopConf.setInt("yarn.resourcemanager.am.max-attempts", 1)
    hadoopConf.set("yarn.scheduler.capacity.root.queues", "default,other_queue")

    hadoopConf.setInt("yarn.scheduler.capacity.root.default.capacity", 100)
    hadoopConf.setInt("yarn.scheduler.capacity.root.default.maximum-capacity", 100)
    hadoopConf.setInt("yarn.scheduler.capacity.root.other_queue.maximum-capacity", 100)

    hadoopConf.setStrings(
      YarnConfiguration.NM_AUX_SERVICES,
      ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID)
    hadoopConf.setClass(
      String.format(
        YarnConfiguration.NM_AUX_SERVICE_FMT,
        ShuffleHandler.MAPREDUCE_SHUFFLE_SERVICEID),
      classOf[ShuffleHandler],
      classOf[Service])

    yarnCluster = new MiniYARNCluster("MiniClusterWordCount", 1, 1, 1)
    logInfo(s"Test working dir ${yarnCluster.getTestWorkDir.getAbsolutePath}")
    yarnCluster.init(hadoopConf)
    yarnCluster.start()
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
    if (yarnCluster != null) {
      yarnCluster.stop()
    }
  }

  test("celeborn mr integration test - word count") {
    val input = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "input")
    Files.write(
      Paths.get(input.getPath, "v1.txt"),
      "hello world celeborn".getBytes(StandardCharsets.UTF_8))
    Files.write(
      Paths.get(input.getPath, "v2.txt"),
      "hello world mapreduce".getBytes(StandardCharsets.UTF_8))

    val output = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "output")
    val mrOutputPath = new Path(output.getPath + File.separator + "mr_output")

    var finish = false
    var exitCode = false
    var retryCount = 0
    while (!finish) {
      try {
        val conf = new Configuration(yarnCluster.getConfig)
        // YARN config
        conf.set(
          "yarn.app.mapreduce.am.command-opts",
          "org.apache.celeborn.mapreduce.v2.app.MRAppMasterWithCeleborn")

        // MapReduce config
        conf.set("mapreduce.framework.name", "yarn")
        conf.set("mapreduce.job.user.classpath.first", "true")

        conf.set(
          "mapreduce.celeborn.master.endpoints",
          s"localhost:${master.conf.get(CelebornConf.MASTER_PORT)}")
        conf.set(
          MRJobConfig.MAP_OUTPUT_COLLECTOR_CLASS_ATTR,
          "org.apache.hadoop.mapred.CelebornMapOutputCollector")
        conf.set(
          "mapreduce.job.reduce.shuffle.consumer.plugin.class",
          "org.apache.hadoop.mapreduce.task.reduce.CelebornShuffleConsumer")

        val job = Job.getInstance(conf, "word count")
        job.setJarByClass(classOf[WordCount])
        job.setMapperClass(classOf[WordCount.TokenizerMapper])
        job.setCombinerClass(classOf[WordCount.IntSumReducer])
        job.setReducerClass(classOf[WordCount.IntSumReducer])
        job.setOutputKeyClass(classOf[Text])
        job.setOutputValueClass(classOf[IntWritable])
        FileInputFormat.addInputPath(job, new Path(input.getPath))
        FileOutputFormat.setOutputPath(job, mrOutputPath)

        val mapreduceLibPath =
          (Utils.getCodeSourceLocation(getClass).split("/").dropRight(1) ++ Array(
            "mapreduce_lib")).mkString("/")
        val excludeJarList =
          Seq(
            "hadoop-client-api",
            "hadoop-client-runtime",
            "hadoop-client-minicluster",
            "celeborn-client-mr-shaded",
            "log4j")
        Files.list(Paths.get(mapreduceLibPath)).iterator().asScala.foreach(path => {
          if (!excludeJarList.exists(path.toFile.getPath.contains(_))) {
            job.addFileToClassPath(new Path(path.toString))
          }
        })
        logInfo(s"Job class path ${job.getFileClassPaths.map(_.toString).mkString(",")}")

        exitCode = job.waitForCompletion(true)
        if (exitCode) {
          finish = true
        } else {
          retryCount += 1
          if (retryCount >= 2) {
            throw new RuntimeException("failed to run wordcount")
          }
        }
      } catch {
        case e: Exception =>
          retryCount += 1
          if (retryCount >= 2) {
            log.error("failed to run wordcount", e)
            throw e
          }
      }
    }
    assert(exitCode, "Returned error code.")

    val outputFilePath = Paths.get(mrOutputPath.toString, "part-r-00000")
    assert(outputFilePath.toFile.exists())
    assert(Files.readAllLines(outputFilePath).contains("celeborn\t1"))
  }

  test("celeborn mr integration test - word count shuffle exception") {
    val input = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "input")
    Files.write(
      Paths.get(input.getPath, "v1.txt"),
      "hello world celeborn".getBytes(StandardCharsets.UTF_8))
    Files.write(
      Paths.get(input.getPath, "v2.txt"),
      "hello world mapreduce".getBytes(StandardCharsets.UTF_8))

    val output = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "output")
    val mrOutputPath = new Path(output.getPath + File.separator + "mr_output")

    var exitCode = false
    val conf = new Configuration(yarnCluster.getConfig)
    // YARN config
    conf.set(
      "yarn.app.mapreduce.am.command-opts",
      "org.apache.celeborn.mapreduce.v2.app.MRAppMasterWithCeleborn")

    // MapReduce config
    conf.set("mapreduce.framework.name", "yarn")
    conf.set("mapreduce.job.user.classpath.first", "true")

    conf.set(
      "mapreduce.celeborn.master.endpoints",
      s"errorhost:${master.conf.get(CelebornConf.MASTER_PORT)}")
    conf.set(
      MRJobConfig.MAP_OUTPUT_COLLECTOR_CLASS_ATTR,
      "org.apache.hadoop.mapred.CelebornMapOutputCollector")
    conf.set(
      "mapreduce.job.reduce.shuffle.consumer.plugin.class",
      "org.apache.hadoop.mapreduce.task.reduce.CelebornShuffleConsumer")

    val job = Job.getInstance(conf, "word count")
    job.setJarByClass(classOf[WordCount])
    job.setMapperClass(classOf[WordCount.TokenizerMapper])
    job.setCombinerClass(classOf[WordCount.IntSumReducer])
    job.setReducerClass(classOf[WordCount.IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(input.getPath))
    FileOutputFormat.setOutputPath(job, mrOutputPath)

    val mapreduceLibPath =
      (Utils.getCodeSourceLocation(getClass).split("/").dropRight(1) ++ Array(
        "mapreduce_lib")).mkString("/")
    val excludeJarList =
      Seq(
        "hadoop-client-api",
        "hadoop-client-runtime",
        "hadoop-client-minicluster",
        "celeborn-client-mr-shaded",
        "log4j")
    Files.list(Paths.get(mapreduceLibPath)).iterator().asScala.foreach(path => {
      if (!excludeJarList.exists(path.toFile.getPath.contains(_))) {
        job.addFileToClassPath(new Path(path.toString))
      }
    })
    logInfo(s"Job class path ${job.getFileClassPaths.map(_.toString).mkString(",")}")

    exitCode = job.waitForCompletion(true)
    assert(!exitCode, "Should return error code.")

    val outputFilePath = Paths.get(mrOutputPath.toString, "part-r-00000")
    assert(!outputFilePath.toFile.exists())
  }
}
