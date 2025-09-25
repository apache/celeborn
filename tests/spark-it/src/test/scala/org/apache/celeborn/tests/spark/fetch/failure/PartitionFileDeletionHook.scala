/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache Software Foundation, Version 2.0
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

package org.apache.celeborn.tests.spark.fetch.failure

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.TaskContext
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ShuffleManagerHook, SparkUtils}

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging

/**
 * Hook to delete specific type of partition files before shuffle read
 * This hook deletes either PRIMARY or REPLICA files based on configuration
 */
class PartitionFileDeletionHook(
    conf: CelebornConf,
    workerDirs: Seq[String],
    deleteReplicaFiles: Boolean)
  extends ShuffleManagerHook with Logging {

  val executed = new AtomicBoolean(false)
  val lock = new Object

  override def exec(
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): Unit = {
    if (executed.get()) {
      return
    }
    lock.synchronized {
      handle match {
        case h: CelebornShuffleHandle[_, _, _] => {
          val appUniqueId = h.appUniqueId
          val shuffleClient = ShuffleClient.get(
            h.appUniqueId,
            h.lifecycleManagerHost,
            h.lifecycleManagerPort,
            conf,
            h.userIdentifier,
            h.extension)
          val celebornShuffleId = SparkUtils.celebornShuffleId(shuffleClient, h, context, false)
          logInfo(s"PartitionFileDeletionHook: Deleting ${if (deleteReplicaFiles) "REPLICA"
          else "PRIMARY"} files for shuffle $celebornShuffleId")
          deletePartitionFiles(appUniqueId, celebornShuffleId, deleteReplicaFiles)
          executed.set(true)
        }
        case x =>
          throw new RuntimeException(s"unexpected, only support CelebornShuffleHandle here," +
            s" but get ${x.getClass.getCanonicalName}")
      }
    }
  }

  private def deletePartitionFiles(
      appUniqueId: String,
      celebornShuffleId: Int,
      deleteReplicaFiles: Boolean): Unit = {
    workerDirs.foreach { dir =>
      val shuffleDataDir =
        new File(s"$dir/celeborn-worker/shuffle_data/$appUniqueId/$celebornShuffleId")
      if (shuffleDataDir.exists()) {
        shuffleDataDir.listFiles().foreach { file =>
          if (file.isFile && shouldDeleteFile(file.getName, deleteReplicaFiles)) {
            logInfo(s"PartitionFileDeletionHook: Deleting file ${file.getName}")
            file.delete()
          }
        }
      }
    }
  }

  /**
   * Determine if a file should be deleted based on its name and deletion mode
   * FileName format: id-epoch-mode (e.g., "1-0-0" for PRIMARY, "1-0-1" for REPLICA)
   */
  private def shouldDeleteFile(fileName: String, deleteReplicaFiles: Boolean): Boolean = {
    val parts = fileName.split("-")
    if (parts.length >= 3) {
      val modeByte = parts(2).toByte
      val isReplicaFile = modeByte == 1

      if (deleteReplicaFiles) {
        isReplicaFile // Delete replica files
      } else {
        !isReplicaFile // Delete primary files
      }
    } else {
      false // Don't delete files with unexpected format
    }
  }
}
