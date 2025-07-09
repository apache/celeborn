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

package org.apache.celeborn.common.network.buffer;

import java.io.File;

import scala.Tuple2;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.network.util.TransportConf;

public class FileChunkBuffers extends ChunkBuffers {
  private final File file;
  private final TransportConf conf;

  public FileChunkBuffers(DiskFileInfo fileInfo, TransportConf conf) {
    super(fileInfo.getReduceFileMeta());
    file = fileInfo.getFile();
    this.conf = conf;
  }

  @Override
  public ManagedBuffer chunk(int chunkIndex, int offset, int len) {
    Tuple2<Long, Long> offsetLen = getChunkOffsetLength(chunkIndex, offset, len);
    return new FileSegmentManagedBuffer(conf, file, offsetLen._1, offsetLen._2);
  }
}
