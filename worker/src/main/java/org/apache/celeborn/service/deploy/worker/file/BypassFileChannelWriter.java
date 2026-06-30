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

package org.apache.celeborn.service.deploy.worker.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.netty.buffer.CompositeByteBuf;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.util.FileChannelUtils;

public class BypassFileChannelWriter extends FileChannelWriter {
  private final FileChannel channel;

  public BypassFileChannelWriter(DiskFileInfo diskFileInfo) throws IOException {
    channel = FileChannelUtils.createWritableFileChannel(diskFileInfo.getFilePath());
  }

  @Override
  public void write(CompositeByteBuf buffer, boolean gatherApiEnabled) throws IOException {
    ByteBuffer[] buffers = buffer.nioBuffers();
    if (gatherApiEnabled) {
      int readableBytes = buffer.readableBytes();
      long written = 0L;
      do {
        written = channel.write(buffers) + written;
      } while (written != readableBytes);
    } else {
      for (ByteBuffer byteBuffer : buffers) {
        while (byteBuffer.hasRemaining()) {
          channel.write(byteBuffer);
        }
      }
    }
  }

  @Override
  public void close(boolean commitFilesFsync) throws IOException {
    try {
      if (commitFilesFsync) {
        channel.force(false);
      }
    } finally {
      channel.close();
    }
  }
}
