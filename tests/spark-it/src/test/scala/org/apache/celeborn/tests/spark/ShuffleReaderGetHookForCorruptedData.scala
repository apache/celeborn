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
import java.util.Random
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.Breaks.{break, breakable}

import org.apache.spark.TaskContext
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ShuffleManagerHook, SparkCommonUtils, SparkUtils}
import org.slf4j.LoggerFactory

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.unsafe.Platform

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
  val random = new Random()

  private def modifyDataFileWithSingleBitFlip(appUniqueId: String, celebornShuffleId: Int): Unit = {
    if (corruptedCount > 0) {
      return
    }

    val minFileSize = 16 // Minimum file size to be considered for corruption

    // Find all potential data files across all worker directories
    val dataFiles =
      workerDirs.flatMap(dir => {
        val shuffleDir =
          new File(s"$dir/celeborn-worker/shuffle_data/$appUniqueId/$celebornShuffleId")
        if (shuffleDir.exists()) {
          shuffleDir.listFiles().filter(_.isFile).filter(_.length() >= minFileSize)
        } else {
          Array.empty[File]
        }
      })

    if (dataFiles.isEmpty) {
      logger.error(
        s"No suitable data files found for appUniqueId=$appUniqueId, shuffleId=$celebornShuffleId")
      return
    }

    // Sort files by size (descending) to prioritize larger files that are more likely to have data
    val sortedFiles = dataFiles.sortBy(-_.length())
    logger.info(s"Found ${sortedFiles.length} data files for corruption testing.")

    // Try to corrupt files one by one until successful
    breakable {
      for (file <- sortedFiles) {
        if (tryCorruptFile(file)) {
          corruptedCount = 1
          break
        }
      }
    }
    // If we couldn't corrupt any file through the normal process, use the fallback method
    if (corruptedCount == 0 && sortedFiles.nonEmpty) {
      logger.warn(
        "Could not find a valid data section in any file. Using safer fallback corruption method.")
      val fileToCorrupt = sortedFiles.head // Take the largest file for fallback
      if (fallbackCorruption(fileToCorrupt)) {
        corruptedCount = 1
      }
    }
  }

  /**
   * Try to corrupt a specific file by finding and corrupting a valid data section.
   * @return true if corruption was successful, false otherwise
   */
  private def tryCorruptFile(file: File): Boolean = {

    val random = new Random()
    logger.info(s"Attempting to corrupt file: ${file.getPath()}, size: ${file.length()} bytes")
    val raf = new RandomAccessFile(file, "rw")
    try {
      val fileSize = raf.length()

      // Find a valid data section in the file
      var position: Long = 0
      val headerSize = 16

      breakable {
        while (position + headerSize <= fileSize) {
          raf.seek(position)
          val sizeBuf = new Array[Byte](headerSize)
          raf.readFully(sizeBuf)

          // Parse header
          val mapId: Int = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET)
          val attemptId: Int = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 4)
          val batchId: Int = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 8)
          val dataSize: Int = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 12)

          logger.info(s"Found header at position $position: mapId=$mapId, attemptId=$attemptId, " +
            s"batchId=$batchId, dataSize=$dataSize")

          // Validate dataSize - should be positive and within file bounds
          if (dataSize > 0 && position + headerSize + dataSize <= fileSize) {
            val dataStartPosition = position + headerSize

            // Choose a random position within the data section
            val offsetInData = random.nextInt(dataSize)
            val corruptPosition = dataStartPosition + offsetInData

            // Read the byte at the position we want to corrupt
            raf.seek(corruptPosition)
            val originalByte = raf.readByte()

            // Flip one bit in the byte (avoid the sign bit for numeric values)
            val bitToFlip = 1 << random.nextInt(7) // bits 0-6 only, avoiding the sign bit
            val corruptedByte = (originalByte ^ bitToFlip).toByte

            // Write back the corrupted byte
            raf.seek(corruptPosition)
            raf.writeByte(corruptedByte)

            logger.info(s"Successfully corrupted byte in file ${file.getName()} at position " +
              s"$corruptPosition: ${originalByte & 0xFF} -> ${corruptedByte & 0xFF} (flipped bit $bitToFlip)")

            return true // Corruption successful
          }

          // Skip to next record: header size + data size (even if data size is 0)
          position += headerSize + Math.max(0, dataSize)
        }
      }

      false // No valid data section found
    } catch {
      case e: Exception =>
        logger.error(s"Error while attempting to corrupt file ${file.getPath()}", e)
        false
    } finally {
      raf.close()
    }
  }

  /**
   * Fallback method for corruption when we can't find a valid data section.
   * This method simply corrupts a byte in the middle of the file.
   */
  private def fallbackCorruption(file: File): Boolean = {
    val raf = new RandomAccessFile(file, "rw")
    try {
      val fileSize = raf.length()

      // Skip first 64 bytes to avoid headers, and corrupt somewhere in the middle of the file
      val safeStartPosition = Math.min(64, fileSize / 4)
      val safeEndPosition = Math.max(safeStartPosition + 1, fileSize - 16)

      // Ensure we have a valid range
      if (safeEndPosition <= safeStartPosition) {
        logger.error(s"File ${file.getName()} too small for safe corruption: $fileSize bytes")
        return false
      }

      val corruptPosition = safeStartPosition +
        random.nextInt((safeEndPosition - safeStartPosition).toInt)

      raf.seek(corruptPosition)
      val originalByte = raf.readByte()

      val bitToFlip = 1 << random.nextInt(7)
      val corruptedByte = (originalByte ^ bitToFlip).toByte

      raf.seek(corruptPosition)
      raf.writeByte(corruptedByte)

      logger.info(s"Used fallback corruption approach on file ${file.getName()}. " +
        s"Corrupted byte at position $corruptPosition: ${originalByte & 0xFF} -> ${corruptedByte & 0xFF}")

      true
    } catch {
      case e: Exception =>
        logger.error(s"Error during fallback corruption of file ${file.getPath()}", e)
        false
    } finally {
      raf.close()
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
