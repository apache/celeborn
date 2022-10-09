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

package org.apache.celeborn.client

import java.io.IOException

import com.google.common.annotations.VisibleForTesting
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import org.apache.celeborn.client.ShuffleClient._
import org.apache.celeborn.client.read.RssInputStream
import org.apache.celeborn.common.RssConf
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.rpc.RpcEndpointRef

abstract class ShuffleClient extends Cloneable {

  def setupMetaServiceRef(host: String, port: Int): Unit

  def setupMetaServiceRef(endpointRef: RpcEndpointRef): Unit

  // Write data to a specific reduce partition
  @throws[IOException]
  def pushData(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      data: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int): Int

  @throws[IOException]
  def prepareForMergeData(shuffleId: Int, mapId: Int, attemptId: Int): Unit

  @throws[IOException]
  def mergeData(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      data: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int): Int

  @throws[IOException]
  def pushMergedData(applicationId: String, shuffleId: Int, mapId: Int, attemptId: Int): Unit

  // Report partition locations written by the completed map task
  @throws[IOException]
  def mapperEnd(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int): Unit

  // Cleanup states of the map task
  def cleanup(applicationId: String, shuffleId: Int, mapId: Int, attemptId: Int): Unit

  // Reduce side read partition which is deduplicated by mapperId+mapperAttemptNum+batchId, batchId
  // is a self-incrementing variable hidden in the implementation when sending data.
  @throws[IOException]
  def readPartition(
      applicationId: String,
      shuffleId: Int,
      partitionId: Int,
      attemptNumber: Int,
      startMapIndex: Int,
      endMapIndex: Int): RssInputStream

  @throws[IOException]
  def readPartition(
      applicationId: String,
      shuffleId: Int,
      partitionId: Int,
      attemptNumber: Int): RssInputStream

  def unregisterShuffle(
      applicationId: String,
      shuffleId: Int,
      isDriver: Boolean): Boolean

  def shutDown(): Unit
}

object ShuffleClient {
  @volatile private var _instance: ShuffleClient = null
  @volatile private var initFinished: Boolean = false
  @volatile private var hdfsFs: FileSystem = null

  // for testing
  @VisibleForTesting
  def reset(): Unit = {
    _instance = null
    initFinished = false
    hdfsFs = null
  }

  def get(
      driverRef: RpcEndpointRef,
      rssConf: RssConf,
      userIdentifier: UserIdentifier): ShuffleClient = {
    if (null == _instance || !initFinished) {
      classOf[ShuffleClient].synchronized {
        if (null == _instance) {
          // During the execution of Spark tasks, each task may be interrupted due to speculative
          // tasks. If the Task is interrupted while obtaining the ShuffleClient and the
          // ShuffleClient is building a singleton, it may cause the MetaServiceEndpoint to not be
          // assigned. An Executor will only construct a ShuffleClient singleton once. At this time,
          // when communicating with MetaService, it will cause a NullPointerException.
          _instance = new ShuffleClientImpl(rssConf, userIdentifier)
          _instance.setupMetaServiceRef(driverRef)
          initFinished = true
        } else if (!initFinished) {
          _instance.shutDown()
          _instance = new ShuffleClientImpl(rssConf, userIdentifier)
          _instance.setupMetaServiceRef(driverRef)
          initFinished = true
        }
      }
    }
    _instance
  }

  def get(
      driverHost: String,
      port: Int,
      conf: RssConf,
      userIdentifier: UserIdentifier): ShuffleClient = {
    if (null == _instance || !initFinished) {
      classOf[ShuffleClient].synchronized {
        if (null == _instance) {
          _instance = new ShuffleClientImpl(conf, userIdentifier)
          _instance.setupMetaServiceRef(driverHost, port)
          initFinished = true
        } else if (!initFinished) {
          _instance.shutDown()
          _instance = new ShuffleClientImpl(conf, userIdentifier)
          _instance.setupMetaServiceRef(driverHost, port)
          initFinished = true
        }
      }
    }
    _instance
  }

  def getHdfsFs(conf: RssConf): FileSystem = {
    if (null == hdfsFs) {
      classOf[ShuffleClient].synchronized {
        if (null == hdfsFs) {
          val hdfsConfiguration = new Configuration
          try hdfsFs = FileSystem.get(hdfsConfiguration)
          catch {
            case e: IOException =>
              System.err.println("Rss initialize hdfs failed.")
              e.printStackTrace(System.err)
          }
        }
      }
    }
    hdfsFs
  }
}
