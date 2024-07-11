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

class StoragePolicy(conf: CelebornConf, storageManager: StorageManager, source: AbstractSource)
  extends Logging {
  var createFileOrder: Option[List[String]] = conf.workerStoragePolicyCreateFilePolicy
  var evictFileOrder: Option[Map[String, List[String]]] = conf.workerStoragePolicyEvictFilePolicy

  def getEvictedFile(
      celebornFile: CelebornFile,
      partitionDataWriterContext: PartitionDataWriterContext): CelebornFile = {
    evictFileOrder.foreach { order =>
      val orderList = order.get(celebornFile.storageType.name())
      if (orderList != null) {
        return createFile(partitionDataWriterContext, orderList)
      }
    }
    null
  }

  def createFile(
      partitionDataWriterContext: PartitionDataWriterContext,
      order: Option[List[String]] = createFileOrder): CelebornFile = {
    logDebug(
      s"create file for ${partitionDataWriterContext.getShuffleKey} ${partitionDataWriterContext.getPartitionLocation.getFileName}")
    val location = partitionDataWriterContext.getPartitionLocation

    def tryCreateFileByType(storageInfoType: StorageInfo.Type): CelebornFile = {
      try {
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
          case StorageInfo.Type.HDD | StorageInfo.Type.SSD | StorageInfo.Type.HDFS | StorageInfo.Type.OSS | StorageInfo.Type.S3 =>
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
      } catch {
        case e: Exception =>
          logError(s"create celeborn file for storage ${storageInfoType} failed", e)
          null
      }
    }

    order.foreach(lst => {
      for (storageStr <- lst) {
        val storageInfoType = StorageInfo.fromStrToType(storageStr)
        val file = tryCreateFileByType(storageInfoType)
        if (file != null) {
          return file
        }
      }
    })

    throw new CelebornIOException(s"Create file failed for ${partitionDataWriterContext}")
  }
}
