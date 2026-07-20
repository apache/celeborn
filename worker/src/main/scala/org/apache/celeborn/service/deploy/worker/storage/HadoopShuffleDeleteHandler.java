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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.celeborn.common.filesystem.HadoopFilesystemProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

public class HadoopShuffleDeleteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopShuffleDeleteHandler.class);
  private final String shuffleServerId;

  private Configuration hadoopConf;

  public HadoopShuffleDeleteHandler(Configuration hadoopConf, String shuffleServerId) {
    this.hadoopConf = hadoopConf;
    this.shuffleServerId = shuffleServerId;
  }


  public void delete(String[] storageBasePaths, String appId, String user) {
    for (String deletePath : storageBasePaths) {
      final Path path = new Path(deletePath);
      boolean isSuccess = false;
      int times = 0;
      int retryMax = 5;
      long start = System.currentTimeMillis();
      LOG.info(
          "Try delete shuffle data in Hadoop FS for appId[{}] of user[{}] with {}",
          appId,
          user,
          path);
      while (!isSuccess && times < retryMax) {
        try {
          FileSystem fileSystem = HadoopFilesystemProvider.getFilesystem(user, path, hadoopConf);
          delete(fileSystem, path, shuffleServerId);
          isSuccess = true;
        } catch (Exception e) {
          if (e instanceof FileNotFoundException) {
            LOG.info("[{}] doesn't exist, ignore it.", path);
            return;
          }
          times++;
          LOG.warn(
              "Can't delete shuffle data for appId[" + appId + "] with " + times + " times", e);
          try {
            Thread.sleep(1000);
          } catch (Exception ex) {
            LOG.warn("Exception happened when Thread.sleep", ex);
          }
        }
      }
      if (isSuccess) {
        LOG.info(
            "Delete shuffle data in Hadoop FS for appId["
                + appId
                + "] with "
                + path
                + " successfully in "
                + (System.currentTimeMillis() - start)
                + " ms");
      } else {
        LOG.info(
            "Failed to delete shuffle data in Hadoop FS for appId["
                + appId
                + "] with "
                + path
                + " in "
                + (System.currentTimeMillis() - start)
                + " ms");
      }
    }
  }

  private void delete(FileSystem fileSystem, Path path, String filePrefix) throws IOException {
    if (filePrefix == null) {
      // For null prefix (used by remote spill cleanup), recursively delete all content including empty directories
      deleteAll(fileSystem, path);
      return;
    }
    FileStatus[] fileStatuses = fileSystem.listStatus(path);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        delete(fileSystem, fileStatus.getPath(), filePrefix);
      } else {
        if (fileStatus.getPath().getName().startsWith(filePrefix)) {
          fileSystem.delete(fileStatus.getPath(), true);
        }
      }
    }
    ContentSummary contentSummary = fileSystem.getContentSummary(path);
    if (contentSummary.getFileCount() == 0) {
      fileSystem.delete(path, true);
    }
  }

  /**
   * Recursively delete all content under the path, including empty directories.
   * This method ensures that the entire directory tree is removed, not just the files.
   *
   * @param fileSystem Hadoop FileSystem
   * @param path Path to delete
   * @throws IOException if deletion fails
   */
  private void deleteAll(FileSystem fileSystem, Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      LOG.info("Path {} doesn't exist, skip deletion", path);
      return;
    }

    FileStatus[] fileStatuses = fileSystem.listStatus(path);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        // Recursively delete subdirectories first
        deleteAll(fileSystem, fileStatus.getPath());
      } else {
        // Delete files
        fileSystem.delete(fileStatus.getPath(), true);
      }
    }

    // Finally delete the directory itself (using false to delete empty directory)
    // Note: delete(path, true) may not work for empty directories in some Hadoop versions
    boolean deleted = fileSystem.delete(path, false);
    if (deleted) {
      LOG.info("Successfully deleted directory: {}", path);
    } else {
      LOG.warn("Failed to delete directory: {}", path);
    }
  }
}
