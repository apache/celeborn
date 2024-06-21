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

package org.apache.celeborn.common.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.util.ShuffleBlockInfoUtils.ShuffleBlockInfo;

public class ShuffleBlockInfoUtilsTest {
  private CompositeByteBuf sortedByteBuf;
  private CompositeByteBuf targetByteBuf;
  private long shuffleChunkSize;

  @Before
  public void setUp() {
    sortedByteBuf = Unpooled.compositeBuffer();
    targetByteBuf = Unpooled.compositeBuffer();
    shuffleChunkSize = 100L;
  }

  @Test
  public void testSliceSortedBufferByMapRangeCase1() {
    Map<Integer, List<ShuffleBlockInfo>> indexMap = new HashMap<>();

    indexMap.put(0, Arrays.asList(new ShuffleBlockInfo(0, 50), new ShuffleBlockInfo(50, 30)));
    indexMap.put(1, Arrays.asList(new ShuffleBlockInfo(0, 20), new ShuffleBlockInfo(20, 30)));

    for (ShuffleBlockInfo blockInfo :
        indexMap.values().stream().flatMap(List::stream).toArray(ShuffleBlockInfo[]::new)) {
      byte[] data = new byte[(int) blockInfo.length];
      Arrays.fill(data, (byte) blockInfo.offset); // Fill data with offset value for simplicity
      sortedByteBuf.addComponents(Unpooled.wrappedBuffer(data));
    }

    ShuffleBlockInfoUtils.sliceSortedBufferByMapRange(
        0, 1, indexMap, sortedByteBuf, targetByteBuf, shuffleChunkSize);

    Assert.assertEquals(
        "Unexpected number of components in target buffer", 1, targetByteBuf.numComponents());
  }

  @Test
  public void testSliceSortedBufferByMapRangeCase2() {
    Map<Integer, List<ShuffleBlockInfo>> indexMap = new HashMap<>();

    indexMap.put(0, Arrays.asList(new ShuffleBlockInfo(0, 50), new ShuffleBlockInfo(50, 50)));
    indexMap.put(1, Arrays.asList(new ShuffleBlockInfo(0, 20), new ShuffleBlockInfo(20, 30)));

    for (ShuffleBlockInfo blockInfo :
        indexMap.values().stream().flatMap(List::stream).toArray(ShuffleBlockInfo[]::new)) {
      byte[] data = new byte[(int) blockInfo.length];
      Arrays.fill(data, (byte) blockInfo.offset); // Fill data with offset value for simplicity
      sortedByteBuf.addComponents(Unpooled.wrappedBuffer(data));
    }

    ShuffleBlockInfoUtils.sliceSortedBufferByMapRange(
        0, 1, indexMap, sortedByteBuf, targetByteBuf, shuffleChunkSize);

    Assert.assertEquals(
        "Unexpected number of components in target buffer", 1, targetByteBuf.numComponents());
  }

  @Test
  public void testSliceSortedBufferByMapRangeCase3() {
    Map<Integer, List<ShuffleBlockInfo>> indexMap = new HashMap<>();

    indexMap.put(0, Arrays.asList(new ShuffleBlockInfo(0, 50), new ShuffleBlockInfo(50, 51)));
    indexMap.put(1, Arrays.asList(new ShuffleBlockInfo(0, 20), new ShuffleBlockInfo(20, 30)));

    for (ShuffleBlockInfo blockInfo :
        indexMap.values().stream().flatMap(List::stream).toArray(ShuffleBlockInfo[]::new)) {
      byte[] data = new byte[(int) blockInfo.length];
      Arrays.fill(data, (byte) blockInfo.offset); // Fill data with offset value for simplicity
      sortedByteBuf.addComponents(Unpooled.wrappedBuffer(data));
    }

    ShuffleBlockInfoUtils.sliceSortedBufferByMapRange(
        0, 1, indexMap, sortedByteBuf, targetByteBuf, shuffleChunkSize);

    Assert.assertEquals(
        "Unexpected number of components in target buffer", 1, targetByteBuf.numComponents());
  }

  @Test
  public void testSliceSortedBufferByMapRangeCase4() {
    Map<Integer, List<ShuffleBlockInfo>> indexMap = new HashMap<>();

    indexMap.put(
        0,
        Arrays.asList(
            new ShuffleBlockInfo(0, 50),
            new ShuffleBlockInfo(50, 51),
            new ShuffleBlockInfo(101, 49),
            new ShuffleBlockInfo(150, 51)));
    indexMap.put(1, Arrays.asList(new ShuffleBlockInfo(0, 20), new ShuffleBlockInfo(20, 30)));

    for (ShuffleBlockInfo blockInfo :
        indexMap.values().stream().flatMap(List::stream).toArray(ShuffleBlockInfo[]::new)) {
      byte[] data = new byte[(int) blockInfo.length];
      Arrays.fill(data, (byte) blockInfo.offset); // Fill data with offset value for simplicity
      sortedByteBuf.addComponents(Unpooled.wrappedBuffer(data));
    }

    ShuffleBlockInfoUtils.sliceSortedBufferByMapRange(
        0, 1, indexMap, sortedByteBuf, targetByteBuf, shuffleChunkSize);

    Assert.assertEquals(
        "Unexpected number of components in target buffer", 2, targetByteBuf.numComponents());
  }

  @Test
  public void testSliceWithEmptyMap() {
    Map<Integer, List<ShuffleBlockInfo>> indexMap = new HashMap<>();

    ShuffleBlockInfoUtils.sliceSortedBufferByMapRange(
        0, 0, indexMap, sortedByteBuf, targetByteBuf, shuffleChunkSize);

    Assert.assertTrue(
        "Target buffer should remain empty with an empty indexMap",
        targetByteBuf.numComponents() == 0);
  }
}
