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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DiskFileInfo, FileInfo}
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.{PartitionType, StorageInfo}

class StoragePolicy(conf: CelebornConf, storageManager: StorageManager, source: AbstractSource)
  extends Logging {
  var createFileOrder: Option[List[String]] = conf.workerStoragePolicyCreateFilePolicy
  var evictFileOrder: Option[Map[String, List[String]]] = conf.workerStoragePolicyEvictFilePolicy

  def getEvictedFileWriter(
      celebornFile: TierWriterBase,
      partitionDataWriterContext: PartitionDataWriterContext,
      partitionType: PartitionType,
      numPendingWrites: AtomicInteger,
      notifier: FlushNotifier): TierWriterBase = {
    evictFileOrder.foreach { order =>
      val orderList = order.get(celebornFile.storageType.name())
      if (orderList != null) {
        return createFileWriter(
          partitionDataWriterContext,
          partitionType,
          numPendingWrites,
          notifier,
          orderList)
      }
    }
    null
  }

  def createFileWriter(
      partitionDataWriterContext: PartitionDataWriterContext,
      partitionType: PartitionType,
      numPendingWrites: AtomicInteger,
      notifier: FlushNotifier,
      order: Option[List[String]] = createFileOrder): TierWriterBase = {
    logDebug(
      s"create file for ${partitionDataWriterContext.getShuffleKey} ${partitionDataWriterContext.getPartitionLocation.getFileName}")
    val location = partitionDataWriterContext.getPartitionLocation

    def getPartitionMetahandler(fileInfo: FileInfo, storageInfoType: StorageInfo.Type) = {
      partitionType match {
        case PartitionType.REDUCE | PartitionType.MAPGROUP =>
          new ReducePartitionMetaHandler(partitionDataWriterContext.isRangeReadFilter, fileInfo)
        case PartitionType.MAP =>
          if (partitionDataWriterContext.isSegmentGranularityVisible) {
            new SegmentMapPartitionMetaHandler(fileInfo.asInstanceOf[DiskFileInfo], notifier)
          } else {
            new MapPartitionMetaHandler(
              fileInfo.asInstanceOf[DiskFileInfo],
              notifier)
          }
      }
    }

    def tryCreateFileByType(storageInfoType: StorageInfo.Type): TierWriterBase = {
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
            storageManager.registerMemoryPartitionWriter(
              partitionDataWriterContext.getPartitionDataWriter,
              memoryFileInfo)
            val metaHandler = getPartitionMetahandler(memoryFileInfo, storageInfoType)
            new MemoryTierWriter(
              conf,
              metaHandler,
              numPendingWrites,
              notifier,
              source,
              memoryFileInfo,
              storageInfoType,
              partitionDataWriterContext,
              storageManager)
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
            val metaHandler = getPartitionMetahandler(diskFileInfo, storageInfoType)
            if (storageInfoType == StorageInfo.Type.HDD || storageInfoType == StorageInfo.Type.SSD) {
              storageManager.registerDiskFilePartitionWriter(
                partitionDataWriterContext.getPartitionDataWriter,
                workingDir,
                diskFileInfo)
              new LocalTierWriter(
                conf,
                metaHandler,
                numPendingWrites,
                notifier,
                flusher,
                source,
                diskFileInfo,
                storageInfoType,
                partitionDataWriterContext,
                storageManager)
            } else {
              new DfsTierWriter(
                conf,
                metaHandler,
                numPendingWrites,
                notifier,
                flusher,
                source,
                diskFileInfo,
                storageInfoType,
                partitionDataWriterContext,
                storageManager)
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
