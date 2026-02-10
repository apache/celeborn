/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.worker.storage

import java.io.ByteArrayInputStream

import io.netty.buffer.{ByteBufAllocator, CompositeByteBuf, UnpooledByteBufAllocator}
import org.apache.commons.io.IOUtils
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchersSugar.eqTo
import org.mockito.MockitoSugar.{verify, _}
import org.scalatest.prop.TableDrivenPropertyChecks.forAll
import org.scalatest.prop.Tables.Table

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler
import org.apache.celeborn.service.deploy.worker.WorkerSource

class FlushTaskSuite extends CelebornFunSuite {

  private val ALLOCATOR: ByteBufAllocator = UnpooledByteBufAllocator.DEFAULT

  test("OSSFlushTask flush should work with buffers of varous sizes") {
    runTest(
      (
          mockBuffer: CompositeByteBuf,
          mockNotifier: FlushNotifier,
          keepBuffer: Boolean,
          mockSource: AbstractSource,
          mockMultipartUploader: MultipartUploadHandler,
          partNumber: Int,
          finalFlush: Boolean) =>
        new OssFlushTask(
          mockBuffer,
          mockNotifier,
          keepBuffer,
          mockSource,
          mockMultipartUploader,
          partNumber,
          finalFlush),
      (mockSource: AbstractSource, expectedLength: Int) => {
        verify(mockSource).incCounter(WorkerSource.OSS_FLUSH_COUNT)
        verify(mockSource).incCounter(WorkerSource.OSS_FLUSH_SIZE, expectedLength)
      })
  }

  test("SSFlushTask flush should work with buffers of varous sizes") {
    runTest(
      (
          mockBuffer: CompositeByteBuf,
          mockNotifier: FlushNotifier,
          keepBuffer: Boolean,
          mockSource: AbstractSource,
          mockMultipartUploader: MultipartUploadHandler,
          partNumber: Int,
          finalFlush: Boolean) =>
        new S3FlushTask(
          mockBuffer,
          mockNotifier,
          keepBuffer,
          mockSource,
          mockMultipartUploader,
          partNumber,
          finalFlush),
      (mockSource: AbstractSource, expectedLength: Int) => {
        verify(mockSource).incCounter(WorkerSource.S3_FLUSH_COUNT)
        verify(mockSource).incCounter(WorkerSource.S3_FLUSH_SIZE, expectedLength)
      })
  }

  def runTest(
      builder: (
          CompositeByteBuf,
          FlushNotifier,
          Boolean,
          AbstractSource,
          MultipartUploadHandler,
          Int,
          Boolean) => DfsFlushTask,
      metricsChecker: (AbstractSource, Int) => Unit) = {
    val bytes = "another test data".getBytes("UTF-8")
    val len = bytes.length

    // Define the scenarios: (scenario name, size to allocate)
    val scenarios = Table(
      ("description", "allocatedSize"),
      ("provider buffer is the same size as the buffer", len),
      ("provider buffer is bigger", len + 10),
      ("provider buffer smaller", len - 5))

    forAll(scenarios) { (description, bufferSize) =>
      val mockBuffer = spy(ALLOCATOR.compositeBuffer())
      mockBuffer.writeBytes(bytes)
      val mockNotifier = mock[FlushNotifier]
      val mockSource = mock[AbstractSource]
      val mockMultipartUploader = mock[MultipartUploadHandler]
      val partNumber = 3
      val finalFlush = false

      val flushTask = builder(
        mockBuffer,
        mockNotifier,
        false, // keepBuffer
        mockSource,
        mockMultipartUploader,
        partNumber,
        finalFlush)

      val copyBytesArray = new Array[Byte](bufferSize)
      flushTask.flush(copyBytesArray)

      if (bufferSize >= bytes.length)
        assert(mockBuffer.readableBytes() == 0)
      else {
        // here buffer position is not moved, because of ByteBufUtil.getBytes
        assert(mockBuffer.readableBytes() == bytes.length)
      }

      val streamCaptor = ArgumentCaptor.forClass(classOf[ByteArrayInputStream])
      verify(mockMultipartUploader).putPart(
        streamCaptor.capture(),
        eqTo(partNumber),
        eqTo(finalFlush))
      metricsChecker(mockSource, bytes.length)

      val capturedStream = streamCaptor.getValue
      val capturedBytes = IOUtils.toByteArray(capturedStream);
      assert(capturedBytes sameElements bytes, s"Content mismatch on: $description")

      mockBuffer.release()
    }
  }
}
