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

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.buffer.ByteBuf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.read.checkpoint.PartitionReaderCheckpointMetadata;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.util.Utils;

public class PartitionReaderErrorHandlingSuiteJ {

  private static final int FETCH_PORT = 10001;

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    ShuffleClient.reset();
  }

  @After
  public void tearDown() {
    ShuffleClient.reset();
  }

  @Test
  public void testDfsBackgroundErrorFailsNext() throws Exception {
    PartitionReaderCheckpointMetadata checkpointMetadata =
        mock(PartitionReaderCheckpointMetadata.class);
    when(checkpointMetadata.getReturnedChunks()).thenReturn(Collections.emptySet());
    AssertionError expected = new AssertionError("dfs fetch failed");
    when(checkpointMetadata.isCheckpointed(anyInt())).thenThrow(expected);

    DfsPartitionReader reader = newDfsReader(checkpointMetadata);
    try {
      Throwable failure = awaitNextFailure(reader);
      assertTrue(failure instanceof CelebornIOException);
      assertSame(expected, failure.getCause());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testLocalBackgroundErrorFailsNext() throws Exception {
    File dataFile = temporaryFolder.newFile("local-partition.data");
    LocalPartitionReader reader = newLocalReader(dataFile);
    FileChannel channel = mock(FileChannel.class);
    AssertionError expected = new AssertionError("local fetch failed");
    when(channel.read(any(ByteBuffer.class))).thenThrow(expected);
    setField(reader, "shuffleChannel", channel);

    try {
      Throwable failure = awaitNextFailure(reader);
      assertTrue(failure instanceof CelebornIOException);
      assertSame(expected, failure.getCause());
      verify(channel).read(any(ByteBuffer.class));
    } finally {
      setField(reader, "shuffleChannel", null);
      reader.close();
    }
  }

  private DfsPartitionReader newDfsReader(PartitionReaderCheckpointMetadata checkpointMetadata)
      throws Exception {
    File dataFile = temporaryFolder.newFile("partition.data");
    try (FileOutputStream output = new FileOutputStream(dataFile)) {
      output.write(1);
    }
    try (DataOutputStream output =
        new DataOutputStream(new FileOutputStream(Utils.getIndexFilePath(dataFile.getPath())))) {
      output.writeInt(2);
      output.writeLong(0L);
      output.writeLong(1L);
    }

    Map<StorageInfo.Type, FileSystem> hadoopFs =
        Collections.singletonMap(StorageInfo.Type.HDFS, FileSystem.getLocal(new Configuration()));
    setStaticField(ShuffleClient.class, "hadoopFs", hadoopFs);

    TransportClientFactory clientFactory = mock(TransportClientFactory.class);
    when(clientFactory.createClient(anyString(), anyInt())).thenReturn(mock(TransportClient.class));
    return new DfsPartitionReader(
        new CelebornConf(),
        "app-1",
        newLocation(StorageInfo.Type.HDFS, dataFile.getAbsolutePath()),
        PbStreamHandler.newBuilder().setStreamId(1L).build(),
        clientFactory,
        0,
        Integer.MAX_VALUE,
        new NoOpMetricsCallback(),
        -1,
        -1,
        Optional.of(checkpointMetadata));
  }

  private LocalPartitionReader newLocalReader(File dataFile) throws Exception {
    TransportClientFactory clientFactory = mock(TransportClientFactory.class);
    when(clientFactory.createClient(anyString(), anyInt(), anyInt()))
        .thenReturn(mock(TransportClient.class));
    PbStreamHandler streamHandler =
        PbStreamHandler.newBuilder()
            .setStreamId(1L)
            .setNumChunks(1)
            .addChunkOffsets(0L)
            .addChunkOffsets(1L)
            .setFullPath(dataFile.getAbsolutePath())
            .build();
    return new LocalPartitionReader(
        new CelebornConf(),
        "app-1",
        newLocation(StorageInfo.Type.HDD, dataFile.getAbsolutePath()),
        streamHandler,
        clientFactory,
        0,
        Integer.MAX_VALUE,
        new NoOpMetricsCallback(),
        -1,
        -1);
  }

  private static Throwable awaitNextFailure(PartitionReader reader) throws Exception {
    ExecutorService caller = Executors.newSingleThreadExecutor();
    Future<ByteBuf> result = caller.submit(reader::next);
    try {
      result.get(5, TimeUnit.SECONDS);
      fail("PartitionReader.next() should fail when its background task fails");
    } catch (ExecutionException e) {
      return e.getCause();
    } catch (TimeoutException e) {
      throw new AssertionError(
          "PartitionReader.next() did not terminate after its background task failed", e);
    } finally {
      result.cancel(true);
      caller.shutdownNow();
    }
    throw new AssertionError("unreachable");
  }

  private static PartitionLocation newLocation(StorageInfo.Type storageType, String path) {
    PartitionLocation location =
        new PartitionLocation(
            0, 0, "localhost", 10000, 10002, FETCH_PORT, 10003, PartitionLocation.Mode.PRIMARY);
    location.setStorageInfo(new StorageInfo(storageType, true, path));
    return location;
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static void setStaticField(Class<?> targetClass, String name, Object value)
      throws Exception {
    Field field = targetClass.getDeclaredField(name);
    field.setAccessible(true);
    field.set(null, value);
  }

  private static class NoOpMetricsCallback implements MetricsCallback {
    @Override
    public void incBytesRead(long bytesRead) {}

    @Override
    public void incReadTime(long time) {}
  }
}
