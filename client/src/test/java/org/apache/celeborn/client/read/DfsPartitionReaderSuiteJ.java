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

package org.apache.celeborn.client.read;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Test;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.util.Utils;

public class DfsPartitionReaderSuiteJ {

  @After
  public void tearDown() {
    ShuffleClient.reset();
  }

  @Test
  public void testZeroChunkPartitionDoesNotOpenMissingDataFile() throws Exception {
    Path tempDir = Files.createTempDirectory("celeborn-dfs-reader");
    String dataFilePath = tempDir.resolve("partition.data").toString();
    Path indexPath = Path.of(Utils.getIndexFilePath(dataFilePath));
    try {
      try (DataOutputStream indexStream = new DataOutputStream(Files.newOutputStream(indexPath))) {
        indexStream.writeInt(1);
        indexStream.writeLong(0L);
      }

      FileSystem localFs = FileSystem.getLocal(new Configuration());
      Map<StorageInfo.Type, FileSystem> hadoopFs = new HashMap<>();
      hadoopFs.put(StorageInfo.Type.S3, localFs);
      setStaticHadoopFs(hadoopFs);

      StorageInfo storageInfo = new StorageInfo(StorageInfo.Type.S3, true, dataFilePath);
      PartitionLocation location =
          new PartitionLocation(
              0, 0, "localhost", 1234, 1235, 1236, 1237, PartitionLocation.Mode.PRIMARY);
      location.setStorageInfo(storageInfo);

      TransportClientFactory clientFactory = mock(TransportClientFactory.class);
      TransportClient client = mock(TransportClient.class);
      when(clientFactory.createClient(location.getHost(), location.getFetchPort()))
          .thenReturn(client);
      when(client.isActive()).thenReturn(false);

      MetricsCallback metricsCallback =
          new MetricsCallback() {
            @Override
            public void incBytesRead(long bytesWritten) {}

            @Override
            public void incReadTime(long time) {}
          };

      DfsPartitionReader reader =
          new DfsPartitionReader(
              new CelebornConf(),
              "shuffle-key",
              location,
              PbStreamHandler.newBuilder().setStreamId(1L).build(),
              clientFactory,
              0,
              Integer.MAX_VALUE,
              metricsCallback,
              -1,
              -1,
              Optional.empty());
      try {
        assertFalse(reader.hasNext());
      } finally {
        reader.close();
      }
    } finally {
      Files.deleteIfExists(indexPath);
      Files.deleteIfExists(tempDir);
    }
  }

  private static void setStaticHadoopFs(Map<StorageInfo.Type, FileSystem> hadoopFs)
      throws Exception {
    Field hadoopFsField = ShuffleClient.class.getDeclaredField("hadoopFs");
    hadoopFsField.setAccessible(true);
    hadoopFsField.set(null, hadoopFs);
  }
}
