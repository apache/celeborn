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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.util.Utils;

public class DfsPartitionDataReader extends PartitionDataReader {

  private final FSDataInputStream dataInputStream;
  private final FSDataInputStream indexInputStream;

  public DfsPartitionDataReader(
      DiskFileInfo fileInfo,
      FSDataInputStream dataInputStream,
      FSDataInputStream indexInputStream,
      ByteBuffer headerBuffer,
      ByteBuffer indexBuffer)
      throws IOException {
    super(fileInfo, headerBuffer, indexBuffer);
    FileSystem fileSystem =
        StorageManager.hadoopFs()
            .get(
                fileInfo.isHdfs()
                    ? StorageInfo.Type.HDFS
                    : fileInfo.isS3() ? StorageInfo.Type.S3 : StorageInfo.Type.OSS);
    this.dataInputStream = dataInputStream;
    this.indexInputStream = indexInputStream;
    this.dataFileSize = fileSystem.getFileStatus(fileInfo.getDfsPath()).getLen();
    this.indexFileSize = fileSystem.getFileStatus(fileInfo.getDfsIndexPath()).getLen();
  }

  @Override
  public void readIndexBuffer(long targetPosition) throws IOException {
    indexInputStream.seek(targetPosition);
    readHeaderOrIndexBuffer(
        indexInputStream,
        indexBuffer,
        indexFileSize,
        indexBuffer.capacity(),
        fileInfo.getIndexPath());
  }

  @Override
  public void position(long targetPosition) throws IOException {
    dataInputStream.seek(targetPosition);
  }

  @Override
  public void readHeaderBuffer(int headerSize) throws IOException {
    readHeaderOrIndexBuffer(
        dataInputStream, headerBuffer, dataFileSize, headerSize, fileInfo.getFilePath());
  }

  @Override
  public void readBufferIntoReadBuffer(ByteBuf buf, long fileSize, int length, String filePath)
      throws IOException {
    Utils.checkFileIntegrity(fileSize - dataInputStream.getPos(), length, filePath);
    ByteBuffer tmpBuffer = ByteBuffer.allocate(length);
    while (tmpBuffer.hasRemaining()) {
      dataInputStream.read(tmpBuffer);
    }
    tmpBuffer.flip();
    buf.writeBytes(tmpBuffer);
  }

  @Override
  public long position() throws IOException {
    return dataInputStream.getPos();
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(dataInputStream);
    IOUtils.closeQuietly(indexInputStream);
  }

  private void readHeaderOrIndexBuffer(
      FSDataInputStream inputStream, ByteBuffer buffer, long fileSize, int length, String filePath)
      throws IOException {
    Utils.checkFileIntegrity(fileSize - inputStream.getPos(), length, filePath);
    buffer.clear();
    buffer.limit(length);
    while (buffer.hasRemaining()) {
      inputStream.read(buffer);
    }
    buffer.flip();
  }
}
