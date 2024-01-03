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

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import scala.Tuple3;
import scala.Tuple4;

import org.mockito.Mockito;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.MapFileMeta;

public class PartitionDataWriterSuiteUtils {
  public static File getTemporaryFile(File tempDir) throws IOException {
    String filename = UUID.randomUUID().toString();
    File temporaryFile = new File(tempDir, filename);
    temporaryFile.createNewFile();
    return temporaryFile;
  }

  public static Tuple4<StorageManager, Flusher, DiskFileInfo, File> prepareTestFileContext(
      File tempDir, UserIdentifier userIdentifier, Flusher flusher, boolean reduceMeta)
      throws IOException {
    File file = getTemporaryFile(tempDir);
    DiskFileInfo fileInfo = new DiskFileInfo(file, userIdentifier);
    if (!reduceMeta) {
      fileInfo.replaceFileMeta(new MapFileMeta(32 * 1024, 10));
    }
    StorageManager storageManager = Mockito.mock(StorageManager.class);
    Mockito.doAnswer(invocation -> new Tuple3<Flusher, DiskFileInfo, File>(flusher, fileInfo, null))
        .when(storageManager)
        .createFile(
            Mockito.any(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyBoolean());
    return new Tuple4<>(storageManager, flusher, fileInfo, null);
  }
}
