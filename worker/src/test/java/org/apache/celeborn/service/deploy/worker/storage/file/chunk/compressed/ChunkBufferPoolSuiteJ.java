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

package org.apache.celeborn.service.deploy.worker.storage.file.chunk.compressed;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.celeborn.service.deploy.worker.file.chunk.compressed.ChunkBufferPool;

public class ChunkBufferPoolSuiteJ {

  // Use distinct prime-ish sizes per test so different tests never share a bucket.
  // The singleton pool is shared across tests; unique sizes prevent cross-test contamination.
  private static final long SIZE_1 = 1009;
  private static final long SIZE_2 = 2003;
  private static final long SIZE_3 = 4001;
  private static final long SIZE_4 = 8009;
  private static final long SIZE_5 = 16007;
  private static final long SIZE_6 = 32003;
  private static final long SIZE_7 = 64007;
  private static final long SIZE_8 = 128021;

  private ChunkBufferPool pool() {
    return ChunkBufferPool.getInstance();
  }

  // ── Test 1: singleton always returns the same instance ─────────────────────

  @Test
  public void testSingletonIdentity() {
    assertSame(ChunkBufferPool.getInstance(), ChunkBufferPool.getInstance());
  }

  // ── Test 2: fresh acquire allocates buffers with correct capacities ─────────

  @Test
  public void testFreshAcquireAllocatesCorrectCapacities() {
    ChunkBufferPool.BufferPair pair = pool().acquire(SIZE_1);
    try {
      assertNotNull(pair.chunkBuffer);
      assertNotNull(pair.compressedBuffer);
      assertEquals(SIZE_1, pair.chunkBuffer.capacity());
      assertEquals(SIZE_1, pair.compressedBuffer.capacity());
      assertEquals(SIZE_1, pair.chunkSize);
    } finally {
      pool().release(pair);
    }
  }

  // ── Test 3: freshly acquired buffers start at position=0, limit=capacity ───

  @Test
  public void testFreshAcquireBuffersAreInClearState() {
    ChunkBufferPool.BufferPair pair = pool().acquire(SIZE_2);
    try {
      assertEquals(0, pair.chunkBuffer.position());
      assertEquals((int) SIZE_2, pair.chunkBuffer.limit());
      assertEquals(0, pair.compressedBuffer.position());
      assertEquals((int) SIZE_2, pair.compressedBuffer.limit());
    } finally {
      pool().release(pair);
    }
  }

  // ── Test 4: release then acquire returns the exact same BufferPair object ───

  @Test
  public void testReleaseAndAcquireReturnsSameObject() {
    ChunkBufferPool.BufferPair pair = pool().acquire(SIZE_3);
    pool().release(pair);
    ChunkBufferPool.BufferPair reacquired = pool().acquire(SIZE_3);
    try {
      assertSame(pair, reacquired);
    } finally {
      pool().release(reacquired);
    }
  }

  // ── Test 5: reacquired buffers have position reset to 0 even if dirty ───────

  @Test
  public void testReacquiredBuffersAreClearedAfterDirtyUse() {
    ChunkBufferPool.BufferPair pair = pool().acquire(SIZE_4);

    // Simulate dirty use: advance positions on both buffers.
    pair.chunkBuffer.position(10);
    pair.compressedBuffer.position(20);

    pool().release(pair);

    ChunkBufferPool.BufferPair reacquired = pool().acquire(SIZE_4);
    try {
      assertEquals(
          "chunkBuffer position should be 0 after reacquire", 0, reacquired.chunkBuffer.position());
      assertEquals(
          "compressedBuffer position should be 0 after reacquire",
          0,
          reacquired.compressedBuffer.position());
      assertEquals((int) SIZE_4, reacquired.chunkBuffer.limit());
      assertEquals((int) SIZE_4, reacquired.compressedBuffer.limit());
    } finally {
      pool().release(reacquired);
    }
  }

  // ── Test 6: different chunk sizes use independent buckets ──────────────────

  @Test
  public void testDifferentSizesUseIndependentBuckets() {
    ChunkBufferPool.BufferPair pairA = pool().acquire(SIZE_5);
    ChunkBufferPool.BufferPair pairB = pool().acquire(SIZE_6);

    // Release A and B in separate buckets.
    pool().release(pairA);
    pool().release(pairB);

    // Reacquiring size A should give back pairA, not pairB.
    ChunkBufferPool.BufferPair reacquiredA = pool().acquire(SIZE_5);
    ChunkBufferPool.BufferPair reacquiredB = pool().acquire(SIZE_6);
    try {
      assertSame(pairA, reacquiredA);
      assertSame(pairB, reacquiredB);
      assertEquals(SIZE_5, reacquiredA.chunkSize);
      assertEquals(SIZE_6, reacquiredB.chunkSize);
    } finally {
      pool().release(reacquiredA);
      pool().release(reacquiredB);
    }
  }

  // ── Test 7: two acquires without intervening release allocate distinct pairs ─

  @Test
  public void testTwoConsecutiveAcquiresReturnDistinctPairs() {
    ChunkBufferPool.BufferPair pair1 = pool().acquire(SIZE_7);
    ChunkBufferPool.BufferPair pair2 = pool().acquire(SIZE_7);
    try {
      assertNotSame(pair1, pair2);
      assertNotSame(pair1.chunkBuffer, pair2.chunkBuffer);
      assertNotSame(pair1.compressedBuffer, pair2.compressedBuffer);
    } finally {
      pool().release(pair1);
      pool().release(pair2);
    }
  }

  // ── Test 8: pool is LIFO — last released is first reacquired ─────────────

  @Test
  public void testPoolIsLifo() {
    ChunkBufferPool.BufferPair first = pool().acquire(SIZE_8);
    ChunkBufferPool.BufferPair second = pool().acquire(SIZE_8);

    // Release first, then second — second is now at the head of the deque.
    pool().release(first);
    pool().release(second);

    ChunkBufferPool.BufferPair got1 = pool().acquire(SIZE_8);
    ChunkBufferPool.BufferPair got2 = pool().acquire(SIZE_8);
    try {
      assertSame("LIFO: second released should be first reacquired", second, got1);
      assertSame("LIFO: first released should be second reacquired", first, got2);
    } finally {
      pool().release(got1);
      pool().release(got2);
    }
  }

  // ── Test 9: buffers are direct ByteBuffers ────────────────────────────────

  @Test
  public void testAcquiredBuffersAreDirect() {
    ChunkBufferPool.BufferPair pair = pool().acquire(1024);
    try {
      assertTrue("chunkBuffer should be direct", pair.chunkBuffer.isDirect());
      assertTrue("compressedBuffer should be direct", pair.compressedBuffer.isDirect());
    } finally {
      pool().release(pair);
    }
  }

  // ── Test 10: released pair's chunkSize matches the bucket it was acquired from

  @Test
  public void testChunkSizeFieldIsPreserved() {
    long size = 3072L;
    ChunkBufferPool.BufferPair pair = pool().acquire(size);
    assertEquals(size, pair.chunkSize);
    pool().release(pair);

    ChunkBufferPool.BufferPair reacquired = pool().acquire(size);
    assertEquals(size, reacquired.chunkSize);
    pool().release(reacquired);
  }

  // ── Test 11: data written before release is invisible after reacquire ────────

  @Test
  public void testWrittenDataNotVisibleAfterReacquire() {
    ChunkBufferPool.BufferPair pair = pool().acquire(512);
    // Write a known byte pattern into chunkBuffer.
    pair.chunkBuffer.put((byte) 0xDE);
    pair.chunkBuffer.put((byte) 0xAD);
    pool().release(pair);

    ChunkBufferPool.BufferPair reacquired = pool().acquire(512);
    try {
      // position is 0 after clear — the buffer is logically empty regardless of stale bytes.
      assertEquals(0, reacquired.chunkBuffer.position());
      assertEquals(512, reacquired.chunkBuffer.limit());
      // Writing from position 0 again must succeed without IndexOutOfBoundsException.
      reacquired.chunkBuffer.put((byte) 0xFF);
      assertEquals(1, reacquired.chunkBuffer.position());
    } finally {
      pool().release(reacquired);
    }
  }

  // ── Test 12: concurrent acquire/release from multiple threads ─────────────

  @Test
  public void testConcurrentAcquireRelease() throws Exception {
    final long size = 256L;
    final int threads = 8;
    final int iterationsPerThread = 500;
    final AtomicInteger errors = new AtomicInteger(0);

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Future<?>> futures = new ArrayList<>(threads);

    for (int t = 0; t < threads; t++) {
      futures.add(
          executor.submit(
              () -> {
                for (int i = 0; i < iterationsPerThread; i++) {
                  ChunkBufferPool.BufferPair pair = null;
                  try {
                    pair = pool().acquire(size);
                    // Verify invariants under concurrent load.
                    if (pair.chunkBuffer.position() != 0) errors.incrementAndGet();
                    if (pair.compressedBuffer.position() != 0) errors.incrementAndGet();
                    if (pair.chunkBuffer.capacity() != (int) size) errors.incrementAndGet();
                    // Simulate work: advance position.
                    pair.chunkBuffer.put((byte) i);
                  } finally {
                    if (pair != null) pool().release(pair);
                  }
                }
              }));
    }

    executor.shutdown();
    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

    for (Future<?> f : futures) {
      f.get(); // rethrow any exception from worker threads
    }
    assertEquals("No invariant violations expected under concurrent load", 0, errors.get());
  }

  // ── Test 13: pool depth grows as more pairs are released ──────────────────

  @Test
  public void testPoolDepthGrowsWithMultipleReleases() {
    final long size = 128L;
    final int count = 5;
    List<ChunkBufferPool.BufferPair> pairs = new ArrayList<>(count);

    // Acquire 5 distinct pairs.
    for (int i = 0; i < count; i++) {
      pairs.add(pool().acquire(size));
    }
    // Verify all are distinct.
    for (int i = 0; i < count; i++) {
      for (int j = i + 1; j < count; j++) {
        assertNotSame(pairs.get(i), pairs.get(j));
      }
    }

    // Release all 5 back.
    for (ChunkBufferPool.BufferPair p : pairs) pool().release(p);

    // Acquire all 5 again — they should all come from the pool (no fresh allocations).
    List<ChunkBufferPool.BufferPair> reacquired = new ArrayList<>(count);
    for (int i = 0; i < count; i++) reacquired.add(pool().acquire(size));
    try {
      for (ChunkBufferPool.BufferPair r : reacquired) {
        assertTrue(
            "Reacquired pair should be one of the originally released pairs", pairs.contains(r));
      }
    } finally {
      for (ChunkBufferPool.BufferPair r : reacquired) pool().release(r);
    }
  }
}
