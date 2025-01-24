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

import io.netty.buffer.UnpooledByteBufAllocator
import org.scalatest.Assertions.convertToEqualizer

import org.apache.celeborn.common.unsafe.Platform

object WriterUtils {

  def generateFlinkFormatData(
      byteBufAllocator: UnpooledByteBufAllocator,
      partitionId: Int) = {
    val dataBuf = byteBufAllocator.buffer(1024)
    // partitionId attemptId batchId size
    dataBuf.writeInt(partitionId)
    dataBuf.writeInt(0)
    dataBuf.writeInt(0)
    dataBuf.writeInt(1008)
    for (i <- 1 to 1008) {
      dataBuf.writeByte(1)
    }
    assert(1024 === dataBuf.readableBytes())
    dataBuf
  }

  def generateSparkFormatData(
      byteBufAllocator: UnpooledByteBufAllocator,
      attemptId: Int) = {
    val dataBuf = byteBufAllocator.buffer(1024)
    val arr = new Array[Byte](1024)
    // mapId attemptId batchId size
    Platform.putInt(arr, Platform.BYTE_ARRAY_OFFSET, attemptId)
    Platform.putInt(arr, Platform.BYTE_ARRAY_OFFSET + 4, 0)
    Platform.putInt(arr, Platform.BYTE_ARRAY_OFFSET + 8, 0)
    Platform.putInt(arr, Platform.BYTE_ARRAY_OFFSET + 12, 1008)

    dataBuf.writeBytes(arr)
    assert(1024 === dataBuf.readableBytes())
    dataBuf
  }
}
