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
import org.apache.hadoop.fs.FSDataOutputStream;

public class FileInfo {
  public final String filePath;
  private final ArrayList<Long> chunkOffsets;
  public final FSDataOutputStream fsDataOutputStream;
  private transient File file;
  private transient boolean isHdfs = false;
  private String indexPath = null;

  public FileInfo(String filePath, ArrayList<Long> chunkOffsets, String indexPath) {
    this.filePath = filePath;
    this.file = new File(filePath);
    this.chunkOffsets = chunkOffsets;
    this.fsDataOutputStream = null;
    this.indexPath = indexPath;
    isHdfs = filePath.startsWith("hdfs://");
  }

  public FileInfo(File file) {
    this.filePath = file.getAbsolutePath();
    this.file = file;
    this.fsDataOutputStream = null;
    this.chunkOffsets = new ArrayList<>();
    chunkOffsets.add(0L);
  }

  public FileInfo(String filePath, FSDataOutputStream outputStream){
    this.filePath = filePath;
    this.file = null;
    this.fsDataOutputStream = outputStream;
    this.chunkOffsets = new ArrayList<>();
    chunkOffsets.add(0L);
    isHdfs = true;
  }

  public synchronized void addChunkOffset(long bytesFlushed) {
    chunkOffsets.add(bytesFlushed);
  }

  public synchronized int numChunks() {
    if (!chunkOffsets.isEmpty()) {
      return chunkOffsets.size() - 1;
    } else {
      return 0;
    }
  }

  public synchronized long getLastChunkOffset() {
    return chunkOffsets.get(chunkOffsets.size() - 1);
  }

  public synchronized long getFileLength() {
    return chunkOffsets.get(chunkOffsets.size() - 1);
  }

  public File getFile() {
    return file;
  }

  public String getFilePath(){
    return filePath;
  }

  public boolean isHdfs(){
    return isHdfs;
  }

  public void setIndexPath(String indexPath) {
    this.indexPath = indexPath;
  }

  public String getIndexPath() {
    return indexPath;
  }

  public synchronized ArrayList<Long> getChunkOffsets() {
    return chunkOffsets;
  }

  @Override
  public String toString() {
    return "FileInfo{" +
             "file=" + filePath +
             ", chunkOffsets=" + StringUtils.join(this.chunkOffsets, ",") +
             '}';
  }
}
