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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import io.netty.buffer.PooledByteBufAllocator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.emr.rss.common.network.util.NettyUtils;
import com.aliyun.emr.rss.common.unsafe.Platform;

public class ShuffleFileSorterSuiteJ {

  private static File shuffleFile;
  private static PooledByteBufAllocator allocator;
  public static final int CHUNK_SIZE = 8 * 1024 * 1024;

  @BeforeClass
  public static void prepare() throws IOException {
    byte[] batchHeader = new byte[16];
    Random random = new Random();
    allocator = NettyUtils.createPooledByteBufAllocator(
      true, true /* allowCache */, 1);
    shuffleFile = File.createTempFile("RSS", "sort-suite");
    System.out.println(shuffleFile.getAbsolutePath());
    FileOutputStream fileOutputStream = new FileOutputStream(shuffleFile);
    FileChannel channel = fileOutputStream.getChannel();
    Map<Integer, Integer> attemptIds = new HashMap<>();
    Map<Integer, Integer> batchIds = new HashMap<>();

    int maxMapId = 50;
    for (int i = 0; i < 1000; i++) {
      int mapId = random.nextInt(maxMapId);
      int currentAttemptId = 0;
      int batchId = batchIds.compute(mapId, (k, v) -> {
        if (v == null) {
          v = 0;
        } else {
          v++;
        }
        return v;
      });
      int dataSize = random.nextInt(192 * 1024) + 65525;
      byte[] mockedData = new byte[dataSize];
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET, mapId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 4, currentAttemptId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 8, batchId);
      Platform.putInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 12, dataSize);
      channel.write(ByteBuffer.wrap(batchHeader));
      random.nextBytes(mockedData);
      channel.write(ByteBuffer.wrap(mockedData));
    }
    MemoryTracker.initialize(0.9, 10, 10);
  }

  @AfterClass
  public static void teardown() {
    shuffleFile.delete();
  }


  @Test
  public void test() {
    ShuffleFileSorter sorter = new ShuffleFileSorter(shuffleFile, CHUNK_SIZE, allocator);
    sorter.sort();
    FileInfo info = sorter.resolve(4, 5);
    assert info.numChunks > 0;
  }
}
