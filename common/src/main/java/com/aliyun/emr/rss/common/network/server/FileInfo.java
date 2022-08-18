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

package com.aliyun.emr.rss.common.network.server;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;

public class FileInfo {
  public final File file;
  public final ArrayList<Long> chunkOffsets;

  public FileInfo(File file, ArrayList<Long> chunkOffsets) {
    this.file = file;
    this.chunkOffsets = chunkOffsets;
  }

  public FileInfo(File file) {
    this.file = file;
    this.chunkOffsets = new ArrayList<>();
    chunkOffsets.add(0L);
  }

  public int numChunks() {
    if (!chunkOffsets.isEmpty()) {
      return chunkOffsets.size() - 1;
    } else {
      return 0;
    }
  }

  public long getFileLength() {
    return chunkOffsets.get(chunkOffsets.size() - 1);
  }

  public File getFile() {
    return file;
  }

  public ArrayList<Long> getChunkOffsets() {
    return chunkOffsets;
  }

  @Override
  public String toString() {
    return "FileInfo{" +
             "file=" + file.getAbsolutePath() +
             ", chunkOffsets=" + StringUtils.join(this.chunkOffsets, ",") +
             '}';
  }
}
