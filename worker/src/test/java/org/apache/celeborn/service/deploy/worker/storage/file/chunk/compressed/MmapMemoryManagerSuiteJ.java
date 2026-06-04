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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.celeborn.service.deploy.worker.file.chunk.compressed.MmapMemoryManager;
import org.junit.Test;

public class MmapMemoryManagerSuiteJ {

  private MmapMemoryManager manager() {
    return MmapMemoryManager.getInstance();
  }

  // ── Test 1: singleton always returns the same instance ─────────────────────

  @Test
  public void testSingletonIdentity() {
    assertSame(manager(), manager());
  }

  // ── Test 2: returned buffer is a direct ByteBuffer ─────────────────────────

  @Test
  public void testAllocatedBufferIsDirect() {
    assertTrue(manager().allocateBuffer(128).isDirect());
  }

  // ── Test 3: capacity equals the requested size ─────────────────────────────

  @Test
  public void testAllocatedBufferCapacityMatchesRequestedSize() {
    int[] sizes = {1, 7, 64, 256, 1024, 8192, 65536};
    for (int size : sizes) {
      ByteBuffer buf = manager().allocateBuffer(size);
      assertEquals("capacity for size " + size, size, buf.capacity());
    }
  }

  // ── Test 4: slice starts at position=0, limit=capacity ─────────────────────

  @Test
  public void testAllocatedBufferIsInClearState() {
    int size = 512;
    ByteBuffer buf = manager().allocateBuffer(size);
    assertEquals(0, buf.position());
    assertEquals(size, buf.limit());
    assertEquals(size, buf.remaining());
  }

  // ── Test 5: buffer is writable — put advances position ──────────────────────

  @Test
  public void testAllocatedBufferIsWritable() {
    ByteBuffer buf = manager().allocateBuffer(64);
    buf.put((byte) 0xAB);
    buf.put((byte) 0xCD);
    assertEquals(2, buf.position());
  }

  // ── Test 6: data round-trips correctly through the buffer ──────────────────

  @Test
  public void testDataRoundTrips() {
    int size = 1024;
    byte[] data = new byte[size];
    new Random(42).nextBytes(data);

    ByteBuffer buf = manager().allocateBuffer(size);
    buf.put(data);
    assertEquals(0, buf.remaining());

    buf.flip();
    byte[] readBack = new byte[size];
    buf.get(readBack);
    assertArrayEquals(data, readBack);
  }

  // ── Test 7: consecutive allocations do not overlap ─────────────────────────
  // Write distinct patterns to two buffers and verify neither corrupts the other.

  @Test
  public void testConsecutiveAllocationsDoNotOverlap() {
    int size = 200;
    ByteBuffer buf1 = manager().allocateBuffer(size);
    ByteBuffer buf2 = manager().allocateBuffer(size);

    for (int i = 0; i < size; i++) buf1.put((byte) 0xAA);
    for (int i = 0; i < size; i++) buf2.put((byte) 0xBB);

    buf1.flip();
    while (buf1.hasRemaining()) assertEquals((byte) 0xAA, buf1.get());

    buf2.flip();
    while (buf2.hasRemaining()) assertEquals((byte) 0xBB, buf2.get());
  }

  // ── Test 8: adjacent writes don't spill into the neighboring allocation ─────

  @Test
  public void testWriteToOneBufferDoesNotSpillIntoAdjacentBuffer() {
    int size = 32;
    ByteBuffer a = manager().allocateBuffer(size);
    ByteBuffer b = manager().allocateBuffer(size);

    // Write 0xFF into every byte of a.
    for (int i = 0; i < size; i++) a.put((byte) 0xFF);

    // Overwrite all of b with 0x00.
    for (int i = 0; i < size; i++) b.put((byte) 0x00);

    // a must still contain 0xFF — b's writes must not have reached a.
    a.flip();
    for (int i = 0; i < size; i++) {
      assertEquals("byte " + i + " in a should be 0xFF after b was written", (byte) 0xFF, a.get());
    }
  }

  // ── Test 9: buffer can be filled to exactly its capacity without overflow ───

  @Test
  public void testBufferCanBeFilledToCapacity() {
    int size = 256;
    ByteBuffer buf = manager().allocateBuffer(size);
    byte[] full = new byte[size];
    new Random(7).nextBytes(full);

    buf.put(full); // must not throw
    assertEquals(0, buf.remaining()); // buffer is exactly full
  }

  // ── Test 10: many allocations of varying sizes all have correct properties ──

  @Test
  public void testManyAllocationsOfVariousSizes() {
    int[] sizes = {1, 3, 17, 100, 512, 4096, 32768};
    for (int size : sizes) {
      ByteBuffer buf = manager().allocateBuffer(size);
      assertEquals("capacity=" + size, size, buf.capacity());
      assertEquals("position=" + size, 0, buf.position());
      assertEquals("limit=" + size, size, buf.limit());
      assertTrue("direct=" + size, buf.isDirect());
    }
  }

  // ── Test 11: sequential pattern survives put/get round-trip ────────────────

  @Test
  public void testSequentialPatternSurvivesRoundTrip() {
    int size = 512;
    ByteBuffer buf = manager().allocateBuffer(size);

    for (int i = 0; i < size; i++) buf.put((byte) (i & 0xFF));

    buf.flip();
    for (int i = 0; i < size; i++) {
      assertEquals("byte " + i, (byte) (i & 0xFF), buf.get());
    }
  }

  // ── Test 12: concurrent allocations are thread-safe ────────────────────────

  @Test
  public void testConcurrentAllocationsAreSafe() throws Exception {
    int threads = 8;
    int perThread = 200;
    int bufSize = 128;
    AtomicInteger violations = new AtomicInteger(0);

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Future<?>> futures = new ArrayList<>(threads);

    for (int t = 0; t < threads; t++) {
      futures.add(
          executor.submit(
              () -> {
                for (int i = 0; i < perThread; i++) {
                  ByteBuffer buf = manager().allocateBuffer(bufSize);
                  if (!buf.isDirect()) violations.incrementAndGet();
                  if (buf.capacity() != bufSize) violations.incrementAndGet();
                  if (buf.position() != 0) violations.incrementAndGet();
                  if (buf.limit() != bufSize) violations.incrementAndGet();
                  // Write and read back a sentinel byte to exercise the mapping.
                  buf.put((byte) 0x5A);
                  buf.flip();
                  if (buf.get() != (byte) 0x5A) violations.incrementAndGet();
                }
              }));
    }

    executor.shutdown();
    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    for (Future<?> f : futures) f.get(); // surface any thread-level exception
    assertEquals("no invariant violations under concurrent load", 0, violations.get());
  }

  // ── Test 13: concurrent writes to different buffers don't corrupt each other ─

  @Test
  public void testConcurrentWritesToDistinctBuffersAreIsolated() throws Exception {
    int threads = 4;
    int size = 256;

    // Pre-allocate one buffer per thread.
    List<ByteBuffer> bufs = new ArrayList<>(threads);
    for (int i = 0; i < threads; i++) bufs.add(manager().allocateBuffer(size));

    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Future<Boolean>> futures = new ArrayList<>(threads);

    for (int t = 0; t < threads; t++) {
      final ByteBuffer buf = bufs.get(t);
      final byte marker = (byte) (t + 1);
      futures.add(
          executor.submit(
              () -> {
                for (int i = 0; i < size; i++) buf.put(marker);
                buf.flip();
                for (int i = 0; i < size; i++) {
                  if (buf.get() != marker) return false;
                }
                return true;
              }));
    }

    executor.shutdown();
    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    for (Future<Boolean> f : futures) {
      assertTrue("each thread's buffer should contain only its own marker", f.get());
    }
  }

  // ── Test 14: close() resets state; subsequent allocations succeed ────────────
  // Named with 'z' prefix so it sorts last alphabetically and runs after all others.

  @Test
  public void zTestCloseResetsStateAndNewAllocationsSucceed() {
    // Allocate something to ensure an active backing file exists.
    ByteBuffer before = manager().allocateBuffer(64);
    assertNotNull(before);

    manager().close();

    // After close, the next allocation must create a new backing file and succeed.
    ByteBuffer after = manager().allocateBuffer(256);
    assertNotNull(after);
    assertEquals(256, after.capacity());
    assertEquals(0, after.position());
    assertEquals(256, after.limit());
    assertTrue(after.isDirect());

    // The buffer must be writable.
    after.put((byte) 0x42);
    after.flip();
    assertEquals((byte) 0x42, after.get());
  }
}
