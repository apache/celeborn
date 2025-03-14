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
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class StoragePolicy(conf: CelebornConf, storageManager: StorageManager, source: AbstractSource)
  extends Logging {
  private val createFileOrder: Option[List[String]] = conf.workerStoragePolicyCreateFilePolicy
  private val evictFileOrder: Option[Map[String, List[String]]] =
    conf.workerStoragePolicyEvictFilePolicy

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
    logError(s"Create evict file failed for ${partitionDataWriterContext.getPartitionLocation}")
    null
  }

  def createFileWriter(
      partitionDataWriterContext: PartitionDataWriterContext,
      partitionType: PartitionType,
      numPendingWrites: AtomicInteger,
      notifier: FlushNotifier): TierWriterBase = {
    createFileWriter(
      partitionDataWriterContext: PartitionDataWriterContext,
      partitionType: PartitionType,
      numPendingWrites: AtomicInteger,
      notifier: FlushNotifier,
      createFileOrder)
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
    if (location == null) {
      throw new IllegalStateException(
        "Partition data writer context can not have null partition location")
    }

    def getPartitionMetaHandler(fileInfo: FileInfo) = {
      partitionType match {
        case PartitionType.REDUCE =>
          new ReducePartitionMetaHandler(partitionDataWriterContext.isRangeReadFilter, fileInfo)
        case PartitionType.MAP =>
          if (partitionDataWriterContext.isSegmentGranularityVisible) {
            new SegmentMapPartitionMetaHandler(fileInfo.asInstanceOf[DiskFileInfo], notifier)
          } else {
            new MapPartitionMetaHandler(
              fileInfo.asInstanceOf[DiskFileInfo],
              notifier)
          }
        case PartitionType.MAPGROUP =>
          null
      }
    }

    def tryCreateFileByType(storageInfoType: StorageInfo.Type): TierWriterBase = {
      try {
        storageInfoType match {
          case StorageInfo.Type.MEMORY =>
            if (location.getStorageInfo.memoryAvailable() && MemoryManager.instance().memoryFileStorageAvailable()) {
              logDebug(s"Create memory file for ${partitionDataWriterContext.getShuffleKey} ${partitionDataWriterContext.getPartitionLocation.getFileName}")
              val memoryFileInfo = storageManager.createMemoryFileInfo(
                partitionDataWriterContext.getAppId,
                partitionDataWriterContext.getShuffleId,
                location.getFileName,
                partitionDataWriterContext.getUserIdentifier,
                partitionDataWriterContext.getPartitionType,
                partitionDataWriterContext.isPartitionSplitEnabled)
              val metaHandler = getPartitionMetaHandler(memoryFileInfo)
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
            } else {
              null
            }
          case StorageInfo.Type.HDD | StorageInfo.Type.SSD | StorageInfo.Type.HDFS | StorageInfo.Type.OSS | StorageInfo.Type.S3 =>
            if (storageManager.localOrDfsStorageAvailable) {
              logDebug(s"create non-memory file for ${partitionDataWriterContext.getShuffleKey} ${partitionDataWriterContext.getPartitionLocation.getFileName}")
              val (flusher, diskFileInfo, workingDir) = storageManager.createDiskFile(
                location,
                partitionDataWriterContext.getAppId,
                partitionDataWriterContext.getShuffleId,
                location.getFileName,
                partitionDataWriterContext.getUserIdentifier,
                partitionDataWriterContext.getPartitionType,
                partitionDataWriterContext.isPartitionSplitEnabled)
              partitionDataWriterContext.setWorkingDir(workingDir)
              val metaHandler = getPartitionMetaHandler(diskFileInfo)
              if (storageInfoType == StorageInfo.Type.HDD || storageInfoType == StorageInfo.Type.SSD) {
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
            } else {
              null
            }
        }
      } catch {
        case e: Exception =>
          logError(s"create celeborn file for storage $storageInfoType failed", e)
          null
      }
    }

    // the fallback order is MEMORY -> LOCAL -> DFS
    if (order.isEmpty) {
      throw new CelebornIOException("Create file order can not be empty, check your configs")
    }

    val tryCreateFileTypeIndex = order.get.indexOf(
      partitionDataWriterContext.getPartitionLocation.getStorageInfo.getType.name())
    val maxSize = order.get.length
    for (i <- tryCreateFileTypeIndex until maxSize) {
      val storageStr = order.get(i)
      val storageInfoType = StorageInfo.fromStrToType(storageStr)
      val file = tryCreateFileByType(storageInfoType)
      if (file != null) {
        return file
      }
    }

    logError(
      s"Could not create file for storage type ${location.getStorageInfo.getType}")

    throw new CelebornIOException(
      s"Create file failed for context ${partitionDataWriterContext.toString}")
  }
}
