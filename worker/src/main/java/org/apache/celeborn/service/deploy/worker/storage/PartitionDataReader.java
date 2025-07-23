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

package org.apache.celeborn.service.deploy.worker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.exception.FileCorruptedException;
import org.apache.celeborn.common.meta.DiskFileInfo;

public abstract class PartitionDataReader {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionDataReader.class);

  protected final DiskFileInfo fileInfo;
  protected final ByteBuffer headerBuffer;
  protected final ByteBuffer indexBuffer;

  protected long dataFileSize;
  protected long indexFileSize;

  public PartitionDataReader(
      DiskFileInfo fileInfo, ByteBuffer headerBuffer, ByteBuffer indexBuffer) {
    this.fileInfo = fileInfo;
    this.headerBuffer = headerBuffer;
    this.indexBuffer = indexBuffer;
  }

  public abstract void readIndexBuffer(long targetPosition) throws IOException;

  public abstract void position(long targetPosition) throws IOException;

  public abstract void readHeaderBuffer(int headSize) throws IOException;

  public abstract void readBufferIntoReadBuffer(
      ByteBuf buf, long fileSize, int length, String filePath) throws IOException;

  public abstract long position() throws IOException;

  public abstract void close();

  public int readBuffer(ByteBuf buffer, long dataConsumingOffset) throws IOException {
    position(dataConsumingOffset);
    int headerSize = headerBuffer.capacity();
    readHeaderBuffer(headerSize);
    // header is combined of mapId(4),attemptId(4),nextBatchId(4) and total Compressed Length(4)
    // we need size here,so we read length directly
    int bufferLength = headerBuffer.getInt(12);
    if (bufferLength <= 0 || bufferLength > buffer.capacity()) {
      LOG.error("Incorrect buffer header, buffer length: {}.", bufferLength);
      throw new FileCorruptedException(
          String.format("File %s is corrupted", fileInfo.getFilePath()));
    }
    buffer.writeBytes(headerBuffer);
    readBufferIntoReadBuffer(buffer, dataFileSize, bufferLength, fileInfo.getFilePath());
    return bufferLength + headerSize;
  }

  public long getDataFileSize() {
    return dataFileSize;
  }

  public long getIndexFileSize() {
    return indexFileSize;
  }
}
