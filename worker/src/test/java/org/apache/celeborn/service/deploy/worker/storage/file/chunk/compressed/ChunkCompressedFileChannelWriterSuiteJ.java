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
import static org.mockito.Mockito.mock;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.github.luben.zstd.ZstdInputStream;
import io.netty.buffer.*;
import org.junit.*;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;
import org.apache.celeborn.common.network.buffer.FileChunkBuffers;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.StorageInfo;

public class ChunkCompressedFileChannelWriterSuiteJ {

  // Small chunk size so tests can easily hit multi-chunk and large-record paths.
  private static final int CHUNK_SIZE = 1024;

  private File tempFile;
  private DiskFileInfo diskFileInfo;
  private TransportConf transportConf;

  @Before
  public void setup() throws Exception {
    tempFile = File.createTempFile("chunk_writer_test", ".tmp");
    tempFile.deleteOnExit();
    diskFileInfo = makeDiskFileInfo(tempFile);
    transportConf = mock(TransportConf.class);
  }

  @After
  public void teardown() {
    tempFile.delete();
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  private DiskFileInfo makeDiskFileInfo(File file) {
    return new DiskFileInfo(
        new UserIdentifier("tenant", "user"),
        true,
        new ReduceFileMeta(new ArrayList<>(Collections.singletonList(0L)), CHUNK_SIZE),
        file.getAbsolutePath(),
        StorageInfo.Type.HDD,
        true);
  }

  /** Wraps one or more strings as a CompositeByteBuf (one component per string). */
  private CompositeByteBuf composite(String... parts) {
    CompositeByteBuf buf = Unpooled.compositeBuffer();
    for (String part : parts) {
      buf.addComponent(true, Unpooled.wrappedBuffer(part.getBytes(StandardCharsets.UTF_8)));
    }
    return buf;
  }

  /** Wraps a raw byte array as a single-component CompositeByteBuf. */
  private CompositeByteBuf compositeOf(byte[] data) {
    CompositeByteBuf buf = Unpooled.compositeBuffer();
    buf.addComponent(true, Unpooled.wrappedBuffer(data));
    return buf;
  }

  /** Returns a byte array of {@code count} repetitions of {@code s}. */
  private byte[] repeat(String s, int count) {
    StringBuilder sb = new StringBuilder(s.length() * count);
    for (int i = 0; i < count; i++) sb.append(s);
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  /** Decompresses one chunk's raw compressed bytes via ZstdInputStream. */
  private byte[] decompress(byte[] compressed) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (InputStream in = new ZstdInputStream(new ByteArrayInputStream(compressed))) {
      byte[] tmp = new byte[4096];
      int n;
      while ((n = in.read(tmp)) != -1) out.write(tmp, 0, n);
    }
    return out.toByteArray();
  }

  /**
   * Reads every chunk from the file (using the updated ReduceFileMeta written by close()),
   * decompresses each one, and returns the list in chunk order.
   */
  private List<byte[]> readChunks() throws Exception {
    FileChunkBuffers buffers = new FileChunkBuffers(diskFileInfo, transportConf);
    int numChunks = buffers.numChunks();
    List<byte[]> result = new ArrayList<>(numChunks);
    for (int i = 0; i < numChunks; i++) {
      ByteBuffer buf = buffers.chunk(i, 0, Integer.MAX_VALUE).nioByteBuffer();
      byte[] compressed = new byte[buf.remaining()];
      buf.get(compressed);
      result.add(decompress(compressed));
    }
    return result;
  }

  /** Concatenates all decompressed chunks into one byte array. */
  private byte[] readAll() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] chunk : readChunks()) out.write(chunk);
    return out.toByteArray();
  }

  // ── Test 1: multiple small buffers — all fit in one chunk ──────────────────

  @Test
  public void testMultipleSmallBuffersProduceOneChunk() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    writer.write(composite("hello", " ", "world"), true);
    writer.write(composite("foo", "bar"), true);
    writer.write(composite("!"), true);
    writer.close(true);

    assertEquals(1, diskFileInfo.getReduceFileMeta().getNumChunks());
    assertArrayEquals("hello worldfoobar!".getBytes(StandardCharsets.UTF_8), readAll());
  }

  // ── Test 2: many small buffers accumulate until overflow forces a new chunk ─

  @Test
  public void testSmallBuffersOverflowIntoSecondChunk() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    // First write nearly fills the chunk buffer (CHUNK_SIZE - 10 bytes).
    byte[] first = repeat("A", CHUNK_SIZE - 10);
    // Second write (50 bytes) overflows → first is flushed as chunk 1, second becomes chunk 2.
    byte[] second = repeat("B", 50);

    writer.write(compositeOf(first), true);
    writer.write(compositeOf(second), true);
    writer.close(true);

    assertEquals(2, diskFileInfo.getReduceFileMeta().getNumChunks());
    List<byte[]> chunks = readChunks();
    assertArrayEquals(first, chunks.get(0));
    assertArrayEquals(second, chunks.get(1));
  }

  // ── Test 3: three sequential small writes spanning three chunks ─────────────

  @Test
  public void testThreeSmallWritesThreeChunks() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    byte[] a = repeat("A", CHUNK_SIZE - 5); // nearly fills chunk 1
    byte[] b = repeat("B", CHUNK_SIZE - 5); // overflows → chunk 1 = a, b nearly fills chunk 2
    byte[] c = repeat("C", 20); // overflows chunk 2 → chunk 2 = b, c is chunk 3

    writer.write(compositeOf(a), true);
    writer.write(compositeOf(b), true);
    writer.write(compositeOf(c), true);
    writer.close(true);

    assertEquals(3, diskFileInfo.getReduceFileMeta().getNumChunks());
    List<byte[]> chunks = readChunks();
    assertArrayEquals(a, chunks.get(0));
    assertArrayEquals(b, chunks.get(1));
    assertArrayEquals(c, chunks.get(2));
  }

  // ── Test 4: write that exactly fills chunkBuffer triggers flush on next write ─

  @Test
  public void testWriteExactlyChunkSizeThenMore() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    byte[] exact = repeat("E", CHUNK_SIZE); // fills chunkBuffer to the brim
    byte[] more = "trailing".getBytes(StandardCharsets.UTF_8);

    writer.write(compositeOf(exact), true); // no flush yet — buffer is full but not overflowed
    writer.write(compositeOf(more), true); // triggers flush of exact; more accumulates
    writer.close(true); // flushes more

    assertEquals(2, diskFileInfo.getReduceFileMeta().getNumChunks());
    List<byte[]> chunks = readChunks();
    assertArrayEquals(exact, chunks.get(0));
    assertArrayEquals(more, chunks.get(1));
  }

  // ── Test 5: large record with no preceding data ─────────────────────────────

  @Test
  public void testLargeRecordAlone() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    // 3× chunkSize — well over the large-record threshold.
    byte[] large = repeat("X", CHUNK_SIZE * 3);
    writer.write(compositeOf(large), true);
    writer.close(true);

    assertEquals(1, diskFileInfo.getReduceFileMeta().getNumChunks());
    assertArrayEquals(large, readAll());
  }

  // ── Test 6: large record just one byte over the threshold ──────────────────

  @Test
  public void testLargeRecordBoundary() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    byte[] boundary = repeat("B", CHUNK_SIZE + 1);
    writer.write(compositeOf(boundary), true);
    writer.close(true);

    assertEquals(1, diskFileInfo.getReduceFileMeta().getNumChunks());
    assertArrayEquals(boundary, readAll());
  }

  // ── Test 7: small write pending, then large record → 2 chunks ──────────────

  @Test
  public void testPendingSmallFlushedBeforeLargeRecord() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    byte[] small = "pending".getBytes(StandardCharsets.UTF_8);
    byte[] large = repeat("L", CHUNK_SIZE * 2);

    writer.write(compositeOf(small), true); // accumulates in chunkBuffer
    writer.write(compositeOf(large), true); // flushes pending small → chunk 1; large → chunk 2
    writer.close(true);

    assertEquals(2, diskFileInfo.getReduceFileMeta().getNumChunks());
    List<byte[]> chunks = readChunks();
    assertArrayEquals(small, chunks.get(0));
    assertArrayEquals(large, chunks.get(1));
  }

  // ── Test 8: two consecutive large records → 2 chunks ──────────────────────

  @Test
  public void testTwoLargeRecords() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    byte[] large1 = repeat("P", CHUNK_SIZE * 2);
    byte[] large2 = repeat("Q", CHUNK_SIZE * 3);

    writer.write(compositeOf(large1), true);
    writer.write(compositeOf(large2), true);
    writer.close(true);

    assertEquals(2, diskFileInfo.getReduceFileMeta().getNumChunks());
    List<byte[]> chunks = readChunks();
    assertArrayEquals(large1, chunks.get(0));
    assertArrayEquals(large2, chunks.get(1));
  }

  // ── Test 9: interleaved small / large / small → 3 chunks ──────────────────

  @Test
  public void testSmallLargeSmallProducesThreeChunks() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    byte[] small1 = "before".getBytes(StandardCharsets.UTF_8);
    byte[] large = repeat("M", CHUNK_SIZE * 2);
    byte[] small2 = "after".getBytes(StandardCharsets.UTF_8);

    writer.write(compositeOf(small1), true); // accumulates → pending
    writer.write(compositeOf(large), true); // flushes small1 as chunk 1; large → chunk 2
    writer.write(compositeOf(small2), true); // accumulates
    writer.close(true); // flushes small2 as chunk 3

    assertEquals(3, diskFileInfo.getReduceFileMeta().getNumChunks());
    List<byte[]> chunks = readChunks();
    assertArrayEquals(small1, chunks.get(0));
    assertArrayEquals(large, chunks.get(1));
    assertArrayEquals(small2, chunks.get(2));
  }

  // ── Test 10: large record followed by small writes ─────────────────────────

  @Test
  public void testLargeRecordThenSmallWrites() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    byte[] large = repeat("R", CHUNK_SIZE * 2);
    byte[] small = "tail".getBytes(StandardCharsets.UTF_8);

    writer.write(compositeOf(large), true); // large → chunk 1
    writer.write(compositeOf(small), true); // accumulates
    writer.close(true); // flushes small → chunk 2

    assertEquals(2, diskFileInfo.getReduceFileMeta().getNumChunks());
    List<byte[]> chunks = readChunks();
    assertArrayEquals(large, chunks.get(0));
    assertArrayEquals(small, chunks.get(1));
  }

  // ── Test 11: no writes at all → 0 chunks ──────────────────────────────────

  @Test
  public void testNoWritesProducesZeroChunks() throws IOException {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);
    writer.close(true);

    assertEquals(0, diskFileInfo.getReduceFileMeta().getNumChunks());
    assertEquals(0L, diskFileInfo.getFileLength());
  }

  // ── Test 12: explicit compressAndFlush mid-stream splits chunks ─────────────

  @Test
  public void testExplicitCompressAndFlushSplitsChunks() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    byte[] part1 = "first part".getBytes(StandardCharsets.UTF_8);
    byte[] part2 = "second part".getBytes(StandardCharsets.UTF_8);

    writer.write(compositeOf(part1), true);
    writer.compressAndFlush(); // explicitly close chunk 1
    writer.write(compositeOf(part2), true);
    writer.close(true); // closes chunk 2

    assertEquals(2, diskFileInfo.getReduceFileMeta().getNumChunks());
    List<byte[]> chunks = readChunks();
    assertArrayEquals(part1, chunks.get(0));
    assertArrayEquals(part2, chunks.get(1));
  }

  // ── Test 13: compressAndFlush on empty buffer is a no-op ──────────────────

  @Test
  public void testCompressAndFlushOnEmptyBufferIsNoop() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    writer.compressAndFlush(); // empty — should not add a chunk
    writer.compressAndFlush(); // again
    writer.write(composite("data"), true);
    writer.compressAndFlush(); // flushes "data" as chunk 1
    writer.compressAndFlush(); // empty again — should not add a chunk
    writer.close(true);

    assertEquals(1, diskFileInfo.getReduceFileMeta().getNumChunks());
    assertArrayEquals("data".getBytes(StandardCharsets.UTF_8), readAll());
  }

  // ── Test 14: fileLength (bytesFlushed) reflects compressed file size ────────

  @Test
  public void testFileLengthMatchesActualFileSize() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    writer.write(composite("hello", " ", "world"), true);
    writer.write(compositeOf(repeat("Z", CHUNK_SIZE * 2)), true);
    writer.close(true);

    assertEquals(tempFile.length(), diskFileInfo.getFileLength());
    assertTrue("File should be non-empty", tempFile.length() > 0);
  }

  // ── Test 15: composite buffer with many small components ──────────────────

  @Test
  public void testCompositeBufferWithManyComponents() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    String[] words = {"alpha", " ", "beta", " ", "gamma", " ", "delta", " ", "epsilon"};
    writer.write(composite(words), true);
    writer.close(true);

    String expected = String.join("", words);
    assertEquals(1, diskFileInfo.getReduceFileMeta().getNumChunks());
    assertEquals(expected, new String(readAll(), StandardCharsets.UTF_8));
  }

  // ── Test 16: chunk offsets are strictly increasing ─────────────────────────

  @Test
  public void testChunkOffsetsAreStrictlyIncreasing() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    writer.write(compositeOf(repeat("A", CHUNK_SIZE - 10)), true);
    writer.write(compositeOf(repeat("B", 50)), true); // triggers chunk 1 flush
    writer.write(compositeOf(repeat("C", CHUNK_SIZE * 2)), true); // large → chunk 3
    writer.close(true);

    List<Long> offsets = diskFileInfo.getReduceFileMeta().getChunkOffsets();
    assertEquals(4, offsets.size()); // [0, end1, end2, end3]
    assertEquals(0L, (long) offsets.get(0));
    for (int i = 1; i < offsets.size(); i++) {
      assertTrue(
          "offset[" + i + "] must be > offset[" + (i - 1) + "]",
          offsets.get(i) > offsets.get(i - 1));
    }
    // Last offset must equal the actual file size.
    assertEquals(tempFile.length(), (long) offsets.get(offsets.size() - 1));
  }

  // ── Test 17: large record with high-entropy data compresses and round-trips ─

  @Test
  public void testLargeRecordHighEntropyData() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    // Pseudo-random high-entropy payload: harder to compress, exercises ZSTD's full path.
    byte[] highEntropy = new byte[CHUNK_SIZE * 4];
    new java.util.Random(42).nextBytes(highEntropy);

    writer.write(compositeOf(highEntropy), true);
    writer.close(true);

    assertEquals(1, diskFileInfo.getReduceFileMeta().getNumChunks());
    assertArrayEquals(highEntropy, readAll());
  }

  // ── Test 18: multiple small writes, one large record, more small writes ─────
  // Exercises the three-phase pattern:
  //   chunk 1 = accumulated smalls flushed before the large record
  //   chunk 2 = the large record as its own ZSTD frame
  //   chunk 3 = trailing smalls flushed on close
  // This is the canonical regression test for the "Unknown frame descriptor" bug
  // where ZstdInputStream was recreated mid-frame on each fillBuffer() call.

  @Test
  public void testMultipleSmallsLargeMultipleSmallsRoundTrip() throws Exception {
    ChunkCompressedFileChannelWriter writer =
        new ChunkCompressedFileChannelWriter(diskFileInfo, CHUNK_SIZE);

    // Phase 1: several small writes that accumulate together into chunk 1.
    // Total = 6+6+1011 = 1023 bytes — just under CHUNK_SIZE (1024).
    byte[] s1 = "alpha-".getBytes(StandardCharsets.UTF_8);  // 6 bytes
    byte[] s2 = "beta--".getBytes(StandardCharsets.UTF_8);  // 6 bytes
    byte[] s3 = repeat("C", CHUNK_SIZE - 13);               // 1011 bytes

    // Phase 2: large record (3× chunkSize).
    // Arriving here triggers compressAndFlush() for the pending smalls (chunk 1),
    // then flushLargeRecord() writes the large data as chunk 2.
    byte[] large = repeat("L", CHUNK_SIZE * 3);

    // Phase 3: a few more small writes that accumulate into chunk 3.
    byte[] s4 = "delta-".getBytes(StandardCharsets.UTF_8);  // 6 bytes
    byte[] s5 = repeat("E", CHUNK_SIZE / 2);                // 512 bytes
    byte[] s6 = "zeta--".getBytes(StandardCharsets.UTF_8);  // 6 bytes

    writer.write(compositeOf(s1), true);
    writer.write(compositeOf(s2), true);
    writer.write(compositeOf(s3), true);
    writer.write(compositeOf(large), true);
    writer.write(compositeOf(s4), true);
    writer.write(compositeOf(s5), true);
    writer.write(compositeOf(s6), true);
    writer.close(true);

    assertEquals(3, diskFileInfo.getReduceFileMeta().getNumChunks());

    List<byte[]> chunks = readChunks();

    // Verify per-chunk content.
    ByteArrayOutputStream expectedChunk1 = new ByteArrayOutputStream();
    expectedChunk1.write(s1);
    expectedChunk1.write(s2);
    expectedChunk1.write(s3);
    assertArrayEquals("chunk 1 must contain all leading small writes", expectedChunk1.toByteArray(), chunks.get(0));

    assertArrayEquals("chunk 2 must contain the large record verbatim", large, chunks.get(1));

    ByteArrayOutputStream expectedChunk3 = new ByteArrayOutputStream();
    expectedChunk3.write(s4);
    expectedChunk3.write(s5);
    expectedChunk3.write(s6);
    assertArrayEquals("chunk 3 must contain all trailing small writes", expectedChunk3.toByteArray(), chunks.get(2));

    // Verify the flat concatenation across all chunks matches the original write order.
    ByteArrayOutputStream all = new ByteArrayOutputStream();
    all.write(s1); all.write(s2); all.write(s3);
    all.write(large);
    all.write(s4); all.write(s5); all.write(s6);
    assertArrayEquals("readAll() must reproduce all data in write order", all.toByteArray(), readAll());
  }
}
