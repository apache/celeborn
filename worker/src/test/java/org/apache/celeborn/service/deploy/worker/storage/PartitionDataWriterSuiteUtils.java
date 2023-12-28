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
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Function0;
import scala.Tuple4;

import org.apache.hadoop.shaded.com.google.common.io.Files;
import org.mockito.Mockito;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.MapFileMeta;
import org.apache.celeborn.common.meta.MemoryFileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

public class PartitionDataWriterSuiteUtils {
  public static File getTemporaryFile(File tempDir) throws IOException {
    String filename = UUID.randomUUID().toString();
    File temporaryFile = new File(tempDir, filename);
    temporaryFile.createNewFile();
    return temporaryFile;
  }

  public static StorageManager prepareDiskFileTestEnvironment(
      File tempDir,
      UserIdentifier userIdentifier,
      Flusher flusher,
      boolean reduceMeta,
      CelebornConf conf)
      throws IOException {
    File file = getTemporaryFile(tempDir);
    DiskFileInfo fileInfo = new DiskFileInfo(file, userIdentifier, conf);
    if (!reduceMeta) {
      fileInfo.replaceFileMeta(
          new MapFileMeta(32 * 1024, 10, false, Collections.emptyMap(), Collections.emptyList()));
    }
    StorageManager storageManager = Mockito.mock(StorageManager.class);
    Mockito.doAnswer(
            invocation ->
                new Tuple4<MemoryFileInfo, Flusher, DiskFileInfo, File>(
                    null, flusher, fileInfo, file))
        .when(storageManager)
        .createFile(Mockito.any(), Mockito.anyBoolean());
    return storageManager;
  }

  public static StorageManager prepareMemoryFileTestEnvironment(
      UserIdentifier userIdentifier,
      boolean reduceMeta,
      StorageManager storageManager,
      CelebornConf celebornConf) {
    ReduceFileMeta reduceFileMeta = new ReduceFileMeta(celebornConf.shuffleChunkSize());
    MemoryFileInfo memoryFileInfo = new MemoryFileInfo(userIdentifier, false, reduceFileMeta);
    if (!reduceMeta) {
      memoryFileInfo.replaceFileMeta(
          new MapFileMeta(32 * 1024, 10, false, Collections.emptyMap(), Collections.emptyList()));
    }

    Mockito.doAnswer(
            invocation ->
                new Tuple4<MemoryFileInfo, Flusher, DiskFileInfo, File>(
                    memoryFileInfo, null, null, null))
        .when(storageManager)
        .createFile(Mockito.any(), Mockito.anyBoolean());
    return storageManager;
  }

  public static StorageManager prepareMemoryEvictEnvironment(
      UserIdentifier userIdentifier,
      boolean reduceMeta,
      StorageManager storageManager,
      CelebornConf celebornConf)
      throws IOException {
    ReduceFileMeta reduceFileMeta = new ReduceFileMeta(celebornConf.shuffleChunkSize());
    MemoryFileInfo memoryFileInfo = new MemoryFileInfo(userIdentifier, false, reduceFileMeta);
    if (!reduceMeta) {
      memoryFileInfo.replaceFileMeta(
          new MapFileMeta(32 * 1024, 10, false, Collections.emptyMap(), Collections.emptyList()));
    }

    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();
    File file = getTemporaryFile(tempDir);
    DiskFileInfo fileInfo = new DiskFileInfo(file, userIdentifier, celebornConf);
    if (!reduceMeta) {
      fileInfo.replaceFileMeta(
          new MapFileMeta(32 * 1024, 10, false, Collections.emptyMap(), Collections.emptyList()));
    }

    AbstractSource source = Mockito.mock(WorkerSource.class);
    Mockito.doAnswer(
            invocationOnMock -> {
              Function0<?> function = (Function0<?>) invocationOnMock.getArguments()[2];
              return function.apply();
            })
        .when(source)
        .sample(Mockito.anyString(), Mockito.anyString(), Mockito.any(Function0.class));
    AtomicInteger callCounter = new AtomicInteger(0);
    LocalFlusher flusher =
        new LocalFlusher(
            source,
            DeviceMonitor$.MODULE$.EmptyMonitor(),
            1,
            NettyUtils.getPooledByteBufAllocator(
                new TransportConf("test", celebornConf), null, true),
            256,
            "disk1",
            StorageInfo.Type.HDD,
            null);
    Mockito.doAnswer(
            invocation -> {
              if (callCounter.getAndIncrement() == 0) {
                return new Tuple4<MemoryFileInfo, Flusher, DiskFileInfo, File>(
                    memoryFileInfo, null, null, null);
              } else {
                return new Tuple4<MemoryFileInfo, Flusher, DiskFileInfo, File>(
                    null, flusher, fileInfo, file);
              }
            })
        .when(storageManager)
        .createFile(Mockito.any(), Mockito.anyBoolean());
    return storageManager;
  }
}
