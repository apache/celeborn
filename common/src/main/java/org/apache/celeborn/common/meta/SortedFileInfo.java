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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.util.Utils;

public class SortedFileInfo extends FileInfo {
  private static final Logger logger = LoggerFactory.getLogger(SortedFileInfo.class);
  private final List<Long> sortedFileLenSumOver;
  private final File[] sortedFiles;

  public SortedFileInfo(
      String filePath,
      List<Long> chunkOffsets,
      UserIdentifier userIdentifier,
      List<Long> sortedFileLenSumOver) {
    super(filePath, chunkOffsets, userIdentifier);
    this.sortedFileLenSumOver = sortedFileLenSumOver;
    this.sortedFiles = new File[sortedFileLenSumOver.size()];
    for (int i = 0; i < sortedFileLenSumOver.size(); i++) {
      sortedFiles[i] = new File(Utils.getSortedFilePath(filePath, i));
    }
  }

  @Override
  public boolean isSortedFile() {
    return true;
  }

  public List<Long> getSortedFileLenSumOver() {
    return sortedFileLenSumOver;
  }

  public File[] getSortedFiles() {
    return sortedFiles;
  }

  @Override
  public void deleteAllFiles(FileSystem hdfsFs) {
    super.deleteAllFiles(hdfsFs);

    if (isHdfs()) {
      try {
        for (File sortedFile : sortedFiles) {
          hdfsFs.delete(new Path(sortedFile.getAbsolutePath()), false);
        }
      } catch (IOException e) {
        logger.debug("delete hdfs sorted files {} failed", getHdfsIndexPath(), e);
      }
    } else {
      for (File sortedFile : sortedFiles) {
        sortedFile.delete();
      }
    }
  }
}
