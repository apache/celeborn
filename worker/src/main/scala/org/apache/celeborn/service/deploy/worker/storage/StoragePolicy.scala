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

package org.apache.celeborn.service.deploy.worker.storage

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.StorageInfo

object StoragePolicy extends Logging {
  var storageManager: StorageManager = _
  var conf: CelebornConf = _
  var createFileOrder: Option[List[String]] = _
  var evictFileOrder: Option[Map[String, List[String]]] = _
  var source: AbstractSource = _

  def initlize(
      conf: CelebornConf,
      storageManager: StorageManager,
      abstractSource: AbstractSource): Unit = {
    this.storageManager = storageManager
    this.conf = conf
    this.createFileOrder = conf.workerStoragePolicyCreateFilePolicy
    this.evictFileOrder = conf.workerStoragePolicyEvictFilePolicy
    this.source = abstractSource
  }

  def getEvictedFile(
      celebornFile: CelebornFile,
      partitionDataWriterContext: PartitionDataWriterContext): CelebornFile = {
    if (evictFileOrder.isDefined) {
      if (evictFileOrder.get.contains(celebornFile.storageType.name())) {
        val order = evictFileOrder.get.get(celebornFile.storageType.name())
        return createFile(partitionDataWriterContext, order)
      }
    }
    null
  }

  def createFile(
      partitionDataWriterContext: PartitionDataWriterContext,
      overrideOrder: Option[List[String]] = null): CelebornFile = {
    logDebug(
      s"create file for ${partitionDataWriterContext.getShuffleKey} ${partitionDataWriterContext.getPartitionLocation.getFileName}")
    val location = partitionDataWriterContext.getPartitionLocation

    def tryCreateFileByType(storageInfoType: StorageInfo.Type): CelebornFile = {
      storageInfoType match {
        case StorageInfo.Type.MEMORY =>
          logDebug(s"Create memory file for ${partitionDataWriterContext.getShuffleKey} ${partitionDataWriterContext.getPartitionLocation.getFileName}")
          val memoryFileInfo = storageManager.createMemoryFileInfo(
            partitionDataWriterContext.getAppId,
            partitionDataWriterContext.getShuffleId,
            location.getFileName,
            partitionDataWriterContext.getUserIdentifier,
            partitionDataWriterContext.getPartitionType,
            partitionDataWriterContext.isPartitionSplitEnabled)
          partitionDataWriterContext.setStorageType(storageInfoType)
          new CelebornMemoryFile(conf, source, memoryFileInfo, storageInfoType)
        case StorageInfo.Type.HDD | StorageInfo.Type.SSD | StorageInfo.Type.HDFS | StorageInfo.Type.OSS =>
          logDebug(s"create non-memory file for ${partitionDataWriterContext.getShuffleKey} ${partitionDataWriterContext.getPartitionLocation.getFileName}")
          val (flusher, diskFileInfo, workingDir) = storageManager.createDiskFile(
            location,
            partitionDataWriterContext.getAppId,
            partitionDataWriterContext.getShuffleId,
            location.getFileName,
            partitionDataWriterContext.getUserIdentifier,
            partitionDataWriterContext.getPartitionType,
            partitionDataWriterContext.isPartitionSplitEnabled)
          if (storageInfoType == StorageInfo.Type.HDD || storageInfoType == StorageInfo.Type.SSD) {
            new CelebornDiskFile(flusher, diskFileInfo, workingDir, storageInfoType)
          } else {
            new CelebornDFSFile(flusher, diskFileInfo, storageInfoType)
          }
      }
    }

    val tmpOrder =
      if (overrideOrder != null) {
        overrideOrder
      } else {
        createFileOrder
      }

    tmpOrder.foreach(lst => {
      for (storageStr <- lst) {
        val storageInfoType = StorageInfo.fromStrToType(storageStr)
        return tryCreateFileByType(storageInfoType)
      }
    })
    throw new CelebornIOException(s"Create file failed for ${partitionDataWriterContext}")
  }
}
