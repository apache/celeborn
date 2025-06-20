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

package org.apache.celeborn.tests.spark

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.util.Random
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.TaskContext
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ShuffleManagerHook, SparkCommonUtils, SparkUtils}
import org.slf4j.LoggerFactory

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

class ShuffleReaderGetHookForCorruptedData(
    conf: CelebornConf,
    workerDirs: Seq[String],
    shuffleIdToBeModified: Seq[Int] = Seq(),
    triggerStageId: Option[Int] = None)
  extends ShuffleManagerHook {

  private val logger = LoggerFactory.getLogger(classOf[ShuffleReaderGetHookForCorruptedData])
  var executed: AtomicBoolean = new AtomicBoolean(false)
  val lock = new Object
  var corruptedCount = 0

  private def modifyDataFileWithSingleBitFlip(appUniqueId: String, celebornShuffleId: Int): Unit = {
    if (corruptedCount > 0) {
      return
    }
    corruptedCount = 1
    val datafile =
      workerDirs.map(dir => {
        new File(s"$dir/celeborn-worker/shuffle_data/$appUniqueId/$celebornShuffleId")
      }).filter(_.exists())
        .flatMap(_.listFiles().iterator).headOption

    datafile match {
      case Some(file) => {
        val raf = new RandomAccessFile(file, "rw")
        try {
          // Get the file size to ensure we have enough data to read
          val fileSize = raf.length()

          if (fileSize >= 16) { // At least one HEADER exists
            // Read the first header to get the size of the DATA section
            val headerBuf = new Array[Byte](16)
            raf.seek(0)
            raf.readFully(headerBuf)

            // Parse the header (assuming little-endian byte order)
            val bb = ByteBuffer.wrap(headerBuf)
            val mapId = bb.getInt()
            val attemptId = bb.getInt()
            val batchId = bb.getInt()
            val dataSize = bb.getInt()

            // Make sure there's actual data to corrupt and we won't exceed file bounds
            val dataStartPosition = 16
            val availableDataSize = Math.min(dataSize, (fileSize - dataStartPosition).toInt)

            if (availableDataSize > 0) {
              // Choose a random position within the available DATA section
              val random = new Random()
              val offsetInData = random.nextInt(availableDataSize)
              val corruptPosition = dataStartPosition + offsetInData

              // Read the byte at the position we want to corrupt
              raf.seek(corruptPosition)
              val originalByte = raf.readByte()

              // Flip one bit in the byte
              val bitToFlip = 1 << random.nextInt(8)
              val corruptedByte = (originalByte ^ bitToFlip).toByte

              // Write back the corrupted byte
              raf.seek(corruptPosition)
              raf.writeByte(corruptedByte)

              logger.info(s"Corrupted byte at position $corruptPosition: " +
                s"${originalByte & 0xFF} -> ${corruptedByte & 0xFF} " +
                s"(flipped bit $bitToFlip)")
            } else {
              logger.info(
                s"Data section is empty or corrupted: dataSize=$dataSize, fileSize=$fileSize")
            }
          } else {
            logger.info(s"File too small (${fileSize} bytes) to contain a complete header")
          }
        } finally {
          raf.close()
        }
      }
      case None => throw new RuntimeException("unexpected, there must be some data file")
    }
  }

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
          val appShuffleIdentifier =
            SparkCommonUtils.encodeAppShuffleIdentifier(handle.shuffleId, context)
          val Array(_, stageId, _) = appShuffleIdentifier.split('-')
          if (triggerStageId.isEmpty || triggerStageId.get == stageId.toInt) {
            if (shuffleIdToBeModified.isEmpty) {
              modifyDataFileWithSingleBitFlip(appUniqueId, celebornShuffleId)
            } else {
              shuffleIdToBeModified.foreach { shuffleId =>
                modifyDataFileWithSingleBitFlip(appUniqueId, shuffleId)
              }
            }
            executed.set(true)
          }
        }
        case x => throw new RuntimeException(s"unexpected, only support RssShuffleHandle here," +
            s" but get ${x.getClass.getCanonicalName}")
      }
    }
  }
}
