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
package org.apache.hadoop.mapred;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;
import org.junit.Test;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;

public class CelebornSortBasedPusherSuiteJ {
  private static class BytesSerializer implements Serializer<byte[]> {
    private OutputStream out;

    @Override
    public void open(OutputStream out) {
      this.out = out;
    }

    @Override
    public void serialize(byte[] value) throws IOException {
      out.write(value);
    }

    @Override
    public void close() {}
  }

  private static class BytesComparator implements RawComparator<byte[]> {
    @Override
    public int compare(byte[] a, byte[] b) {
      return compare(a, 0, a.length, b, 0, b.length);
    }

    @Override
    public int compare(byte[] a, int aOffset, int aLength, byte[] b, int bOffset, int bLength) {
      return WritableComparator.compareBytes(a, aOffset, aLength, b, bOffset, bLength);
    }
  }

  private static class Record {
    final int partition;
    final byte[] key;
    final byte[] value;

    Record(int partition, byte[] key, byte[] value) {
      this.partition = partition;
      this.key = key;
      this.value = value;
    }
  }

  private final List<Record> pushed = new ArrayList<>();
  private final ShuffleClient client = mock(ShuffleClient.class);
  private final Counters.Counter bytes = mock(Counters.Counter.class);
  private final Counters.Counter records = mock(Counters.Counter.class);

  private CelebornSortBasedPusher<byte[], byte[]> newPusher() throws IOException {
    when(client.pushData(
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            any(byte[].class),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt()))
        .thenAnswer(
            invocation -> {
              int partition = invocation.getArgument(3);
              byte[] data = invocation.getArgument(4);
              DataInputStream in =
                  new DataInputStream(new ByteArrayInputStream(data, 4, data.length - 4));
              while (true) {
                int keyLength = WritableUtils.readVInt(in);
                int valueLength = WritableUtils.readVInt(in);
                if (keyLength == -1 && valueLength == -1) {
                  break;
                }
                byte[] key = new byte[keyLength];
                byte[] value = new byte[valueLength];
                in.readFully(key);
                in.readFully(value);
                pushed.add(new Record(partition, key, value));
              }
              assertEquals(0, in.available());
              return data.length;
            });
    return new CelebornSortBasedPusher<>(
        1,
        2,
        0,
        0,
        new BytesSerializer(),
        new BytesSerializer(),
        10,
        9,
        new BytesComparator(),
        bytes,
        records,
        client,
        new CelebornConf());
  }

  private void assertRecord(int index, int partition, byte[] key, byte[] value) {
    Record record = pushed.get(index);
    assertEquals(partition, record.partition);
    assertArrayEquals(key, record.key);
    assertArrayEquals(value, record.value);
  }

  @Test
  public void testBufferFillsDuringValueSerialization() throws Exception {
    CelebornSortBasedPusher<byte[], byte[]> pusher = newPusher();
    byte[] firstKey = {3};
    byte[] firstValue = {1, 2, 3, 4, 5, 6};
    byte[] nextKey = {2};
    byte[] nextValue = {7, 8, 9, 10};
    pusher.insert(firstKey, firstValue, 0);
    pusher.insert(nextKey, nextValue, 1);
    pusher.checkException();
    pusher.flush();
    pusher.checkException();
    assertEquals(2, pushed.size());
    assertRecord(0, 0, firstKey, firstValue);
    assertRecord(1, 1, nextKey, nextValue);
    org.mockito.Mockito.verify(bytes).increment(7);
    org.mockito.Mockito.verify(bytes).increment(5);
    org.mockito.Mockito.verify(records, org.mockito.Mockito.times(2)).increment(1);
  }

  @Test
  public void testBufferFillsDuringKeySerialization() throws Exception {
    CelebornSortBasedPusher<byte[], byte[]> pusher = newPusher();
    byte[] firstKey = {1};
    byte[] firstValue = {2, 3, 4, 5, 6, 7};
    byte[] nextKey = {8, 9, 10, 11};
    byte[] nextValue = {12};
    pusher.insert(firstKey, firstValue, 0);
    pusher.insert(nextKey, nextValue, 1);
    pusher.checkException();
    pusher.flush();
    pusher.checkException();
    assertEquals(2, pushed.size());
    assertRecord(0, 0, firstKey, firstValue);
    assertRecord(1, 1, nextKey, nextValue);
  }

  @Test
  public void testCompleteRecordsRemainSortedWhenBufferFills() throws Exception {
    CelebornSortBasedPusher<byte[], byte[]> pusher = newPusher();
    pusher.insert(new byte[] {3}, new byte[] {30, 31}, 0);
    pusher.insert(new byte[] {1}, new byte[] {10, 11, 12}, 0);
    pusher.insert(new byte[] {2, 2, 2, 2}, new byte[] {20}, 0);
    pusher.checkException();
    pusher.flush();
    pusher.checkException();
    assertEquals(3, pushed.size());
    assertRecord(0, 0, new byte[] {1}, new byte[] {10, 11, 12});
    assertRecord(1, 0, new byte[] {3}, new byte[] {30, 31});
    assertRecord(2, 0, new byte[] {2, 2, 2, 2}, new byte[] {20});
  }

  @Test
  public void testRecordExactlyFillsBuffer() throws Exception {
    CelebornSortBasedPusher<byte[], byte[]> pusher = newPusher();
    pusher.insert(new byte[] {1}, new byte[9], 0);
    pusher.checkException();
    pusher.insert(new byte[] {2}, new byte[] {3}, 1);
    pusher.checkException();
    pusher.flush();
    pusher.checkException();
    assertEquals(2, pushed.size());
    assertRecord(0, 0, new byte[] {1}, new byte[9]);
    assertRecord(1, 1, new byte[] {2}, new byte[] {3});
  }

  @Test
  public void testRecordLargerThanBufferFails() throws Exception {
    CelebornSortBasedPusher<byte[], byte[]> pusher = newPusher();
    pusher.insert(new byte[] {1}, new byte[10], 0);
    try {
      pusher.checkException();
      fail("An oversized record should fail");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage().contains("Sort pusher memory exhausted"));
    }
    assertTrue(pushed.isEmpty());
  }

  @Test
  public void testOversizedRecordAfterCompleteRecord() throws Exception {
    CelebornSortBasedPusher<byte[], byte[]> pusher = newPusher();
    pusher.insert(new byte[] {1}, new byte[] {2, 3, 4, 5, 6, 7}, 0);
    pusher.insert(new byte[] {8}, new byte[10], 1);
    try {
      pusher.checkException();
      fail("An oversized record should fail");
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage().contains("Sort pusher memory exhausted"));
    }
    assertEquals(1, pushed.size());
    assertRecord(0, 0, new byte[] {1}, new byte[] {2, 3, 4, 5, 6, 7});
  }

  @Test
  public void testPushFailureDuringSerializationIsReported() throws Exception {
    CelebornSortBasedPusher<byte[], byte[]> pusher = newPusher();
    org.mockito.Mockito.doThrow(new IOException("push failed"))
        .when(client)
        .pushData(
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            any(byte[].class),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt());
    pusher.insert(new byte[] {1}, new byte[] {2, 3, 4, 5, 6, 7}, 0);
    pusher.insert(new byte[] {8}, new byte[] {9, 10, 11, 12}, 1);
    try {
      pusher.checkException();
      fail("A push failure should be reported");
    } catch (IOException e) {
      assertEquals("push failed", e.getCause().getMessage());
    }
  }
}
