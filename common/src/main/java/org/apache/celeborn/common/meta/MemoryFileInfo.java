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

package org.apache.celeborn.common.meta;

import io.netty.buffer.CompositeByteBuf;

import org.apache.celeborn.common.identity.UserIdentifier;

public class MemoryFileInfo extends FileInfo {
  private CompositeByteBuf buffer;
  private long length;

  public MemoryFileInfo(
      UserIdentifier userIdentifier, boolean partitionSplitEnabled, FileMeta fileMeta) {
    super(userIdentifier, partitionSplitEnabled, fileMeta);
  }

  public CompositeByteBuf getBuffer() {
    return buffer;
  }

  public void setBuffer(CompositeByteBuf buffer) {
    this.buffer = buffer;
  }

  public void setBufferSize(int bufferSize) {
    this.length = bufferSize;
  }

  @Override
  public long getFileLength() {
    return length;
  }
}
