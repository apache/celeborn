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
import java.nio.channels.FileChannel;

import io.netty.buffer.ByteBuf;
import org.apache.commons.io.IOUtils;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.util.Utils;

public class LocalPartitionDataReader extends PartitionDataReader {

  private final FileChannel dataFileChanel;
  private final FileChannel indexFileChannel;

  public LocalPartitionDataReader(
      DiskFileInfo fileInfo,
      FileChannel dataFileChannel,
      FileChannel indexFileChannel,
      ByteBuffer headerBuffer,
      ByteBuffer indexBuffer)
      throws IOException {
    super(fileInfo, headerBuffer, indexBuffer);
    this.dataFileChanel = dataFileChannel;
    this.indexFileChannel = indexFileChannel;
    this.dataFileSize = dataFileChanel.size();
    this.indexFileSize = indexFileChannel.size();
  }

  @Override
  public void readIndexBuffer(long targetPosition) throws IOException {
    indexFileChannel.position(targetPosition);
    readHeaderOrIndexBuffer(
        indexFileChannel,
        indexBuffer,
        indexFileSize,
        indexBuffer.capacity(),
        fileInfo.getIndexPath());
  }

  @Override
  public void position(long targetPosition) throws IOException {
    dataFileChanel.position(targetPosition);
  }

  @Override
  public void readHeaderBuffer(int headerSize) throws IOException {
    readHeaderOrIndexBuffer(
        dataFileChanel, headerBuffer, dataFileSize, headerSize, fileInfo.getFilePath());
  }

  @Override
  public void readBufferIntoReadBuffer(ByteBuf buf, long fileSize, int length, String filePath)
      throws IOException {
    Utils.checkFileIntegrity(fileSize - dataFileChanel.position(), length, filePath);
    ByteBuffer tmpBuffer = ByteBuffer.allocate(length);
    while (tmpBuffer.hasRemaining()) {
      dataFileChanel.read(tmpBuffer);
    }
    tmpBuffer.flip();
    buf.writeBytes(tmpBuffer);
  }

  @Override
  public long position() throws IOException {
    return dataFileChanel.position();
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(dataFileChanel);
    IOUtils.closeQuietly(indexFileChannel);
  }

  private void readHeaderOrIndexBuffer(
      FileChannel channel, ByteBuffer buffer, long fileSize, int length, String filePath)
      throws IOException {
    Utils.checkFileIntegrity(fileSize - channel.position(), length, filePath);
    buffer.clear();
    buffer.limit(length);
    while (buffer.hasRemaining()) {
      channel.read(buffer);
    }
    buffer.flip();
  }
}
