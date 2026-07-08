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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.client.compress.Compressor;
import org.apache.celeborn.client.security.CryptoHandler;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.CommitMetadata;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.ChunkReceivedCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.unsafe.Platform;

/**
 * Integration-style round-trip tests for EAR (Encryption At Rest) wiring in {@link
 * CelebornInputStream}. These tests verify that the encrypt-on-write / decrypt-on-read path works
 * end-to-end, including interactions with compression and the shuffle integrity check.
 */
public class CelebornInputStreamCryptoRoundTripSuiteJ {

  private static final int BATCH_HEADER_SIZE = 16;
  private static final String SHUFFLE_KEY = "app-1-1";

  /**
   * A minimal CryptoHandler for testing: the encrypted format is [4-byte plaintext length
   * (int)][XOR-encrypted payload]. This matches the structural contract of SparkCryptoHandler so
   * the bounds check (decryptedLength > length - 4) is also exercised.
   */
  static class XorCryptoHandler implements CryptoHandler {
    private final byte key;

    XorCryptoHandler(byte key) {
      this.key = key;
    }

    @Override
    public byte[] encrypt(byte[] input, int offset, int length) throws IOException {
      // Prefix with 4-byte plaintext length, then XOR-encrypt the payload
      byte[] out = new byte[4 + length];
      Platform.putInt(out, Platform.BYTE_ARRAY_OFFSET, length);
      for (int i = 0; i < length; i++) {
        out[4 + i] = (byte) (input[offset + i] ^ key);
      }
      return out;
    }

    @Override
    public byte[] decrypt(byte[] input, int offset, int length) throws IOException {
      // Validate the buffer is large enough to hold the 4-byte length prefix
      if (length < 4) {
        throw new IOException("Encrypted buffer too short: " + length);
      }
      // Read the plaintext length from the 4-byte prefix
      int decryptedLength = Platform.getInt(input, Platform.BYTE_ARRAY_OFFSET + offset);
      // Validate bounds: the 4-byte prefix must fit inside the encrypted buffer
      if (decryptedLength < 0 || decryptedLength > length - 4) {
        throw new IOException(
            "Invalid decrypted length: " + decryptedLength + ", encrypted length: " + length);
      }
      byte[] out = new byte[decryptedLength];
      for (int i = 0; i < decryptedLength; i++) {
        out[i] = (byte) (input[offset + 4 + i] ^ key);
      }
      return out;
    }
  }

  /**
   * Build a single batch ByteBuf as ShuffleClientImpl.pushOrMergeData does: optionally compress,
   * optionally encrypt, then prepend the 16-byte batch header.
   */
  private ByteBuf buildBatch(
      byte[] plaintext, boolean compress, CryptoHandler cryptoHandler, CelebornConf conf)
      throws IOException {
    byte[] data = plaintext;
    int offset = 0;
    int length = plaintext.length;

    // Step 1: optionally compress (compress-then-encrypt ordering matches ShuffleClientImpl)
    if (compress) {
      Compressor compressor = Compressor.getCompressor(conf);
      compressor.compress(data, offset, length);
      data = compressor.getCompressedBuffer();
      offset = 0;
      length = compressor.getCompressedTotalSize();
    }

    // Step 2: optionally encrypt the (possibly compressed) payload
    if (cryptoHandler != null) {
      data = cryptoHandler.encrypt(data, offset, length);
      offset = 0;
      length = data.length;
    }

    // Step 3: prepend the 16-byte batch header [mapId|attemptId|batchId|payloadLen]
    byte[] body = new byte[BATCH_HEADER_SIZE + length];
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET, 0); // mapId
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 4, 0); // attemptId
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 8, 0); // batchId
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 12, length); // payload length
    System.arraycopy(data, offset, body, BATCH_HEADER_SIZE, length);
    return Unpooled.wrappedBuffer(body);
  }

  /**
   * Create a CelebornInputStream backed by a mock TransportClient that serves the given batchBuf as
   * a single chunk.
   */
  private CelebornInputStream createStream(
      ByteBuf batchBuf,
      boolean needDecompress,
      Optional<CryptoHandler> cryptoHandler,
      CelebornConf conf)
      throws IOException, InterruptedException {
    return createStreamWithClient(
        batchBuf, needDecompress, cryptoHandler, conf, mock(ShuffleClient.class));
  }

  /**
   * Like {@link #createStream} but with a caller-supplied ShuffleClient mock, so tests can verify
   * interactions such as {@code readReducerPartitionEnd}.
   */
  private CelebornInputStream createStreamWithClient(
      ByteBuf batchBuf,
      boolean needDecompress,
      Optional<CryptoHandler> cryptoHandler,
      CelebornConf conf,
      ShuffleClient shuffleClient)
      throws IOException, InterruptedException {
    TransportClient client = mock(TransportClient.class);
    PbStreamHandler pbHandler =
        PbStreamHandler.newBuilder().setStreamId(1L).setNumChunks(1).build();
    // Encode the stream handler into an RPC response that CelebornInputStream expects
    ByteBuffer rpcResponse =
        new TransportMessage(MessageType.STREAM_HANDLER, pbHandler.toByteArray()).toByteBuffer();
    when(client.sendRpcSync(any(ByteBuffer.class), anyLong())).thenReturn(rpcResponse);
    doNothing().when(client).sendRpc(any(ByteBuffer.class));
    doAnswer(
            invocation -> {
              ChunkReceivedCallback cb = invocation.getArgument(3);
              // Serve the pre-built batch buffer immediately as chunk 0; duplicate() shares
              // the underlying data without incrementing the ref count, so the stream's
              // single release correctly frees the buffer.
              cb.onSuccess(0, new NettyManagedBuffer(batchBuf.duplicate()));
              return null;
            })
        .when(client)
        .fetchChunk(anyLong(), anyInt(), anyLong(), any(ChunkReceivedCallback.class));

    TransportClientFactory clientFactory = mock(TransportClientFactory.class);
    when(clientFactory.createClient(anyString(), anyInt())).thenReturn(client);

    // PRIMARY location pointing to a single HDD partition
    PartitionLocation location =
        new PartitionLocation(
            0, 0, "host1", 9001, 9002, 9003, 9004, PartitionLocation.Mode.PRIMARY);
    location.setStorageInfo(new StorageInfo(StorageInfo.Type.HDD, true, "/mnt/disk1"));

    ArrayList<PartitionLocation> locations = new ArrayList<>();
    locations.add(location);
    ArrayList<PbStreamHandler> handlers = new ArrayList<>();
    handlers.add(PbStreamHandler.newBuilder().setStreamId(1L).setNumChunks(1).build());

    return CelebornInputStream.create(
        conf,
        clientFactory,
        SHUFFLE_KEY,
        locations,
        handlers,
        new int[] {0},
        new HashMap<>(),
        new HashMap<>(),
        0,
        1L,
        0,
        100,
        new ConcurrentHashMap<>(),
        shuffleClient,
        1,
        1,
        0,
        null,
        new MetricsCallback() {
          @Override
          public void incBytesRead(long bytes) {}

          @Override
          public void incReadTime(long time) {}
        },
        needDecompress,
        cryptoHandler);
  }

  private byte[] readAll(CelebornInputStream stream) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[4096];
    int n;
    while ((n = stream.read(buf)) != -1) {
      baos.write(buf, 0, n);
    }
    return baos.toByteArray();
  }

  @Test
  public void testEncryptDecryptRoundTrip() throws IOException, InterruptedException {
    byte[] plaintext = "hello, EAR round-trip without compression".getBytes();
    CelebornConf conf = new CelebornConf();
    XorCryptoHandler handler = new XorCryptoHandler((byte) 0x5A);

    // Build an encrypted batch and read it back through CelebornInputStream
    ByteBuf batchBuf = buildBatch(plaintext, false, handler, conf);
    try (CelebornInputStream stream = createStream(batchBuf, false, Optional.of(handler), conf)) {
      assertArrayEquals(plaintext, readAll(stream));
    }
  }

  @Test
  public void testNoEncryptionRoundTrip() throws IOException, InterruptedException {
    byte[] plaintext = "unencrypted shuffle data sanity check".getBytes();
    CelebornConf conf = new CelebornConf();

    // Baseline: with no CryptoHandler the data flows through unchanged
    ByteBuf batchBuf = buildBatch(plaintext, false, null, conf);
    try (CelebornInputStream stream = createStream(batchBuf, false, Optional.empty(), conf)) {
      assertArrayEquals(plaintext, readAll(stream));
    }
  }

  @Test
  public void testCompressThenEncryptRoundTrip() throws IOException, InterruptedException {
    // Reproduce the compress-then-encrypt ordering used in ShuffleClientImpl.
    byte[] plaintext = "shuffle data with compression and encryption enabled for EAR".getBytes();
    CelebornConf conf = new CelebornConf();
    // Use LZ4 (default)
    conf.set(CelebornConf.SHUFFLE_COMPRESSION_CODEC().key(), "lz4");
    XorCryptoHandler handler = new XorCryptoHandler((byte) 0x3C);

    // Writer: LZ4-compress then XOR-encrypt; Reader: decrypt then decompress
    ByteBuf batchBuf = buildBatch(plaintext, true, handler, conf);
    try (CelebornInputStream stream = createStream(batchBuf, true, Optional.of(handler), conf)) {
      assertArrayEquals(plaintext, readAll(stream));
    }
  }

  @Test
  public void testEncryptWithIntegrityCheckEnabled() throws IOException, InterruptedException {
    // Verify that EAR + shuffle integrity check work together: the checksum must be computed
    // over the *decrypted* plaintext, not the ciphertext. We capture the crc32/bytes passed to
    // readReducerPartitionEnd and assert they match an independently-computed plaintext checksum.
    byte[] plaintext = "integrity check should pass after decryption".getBytes();
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.CLIENT_SHUFFLE_INTEGRITY_CHECK_ENABLED().key(), "true");
    XorCryptoHandler handler = new XorCryptoHandler((byte) 0x7F);

    // Independently compute the expected checksum over the plaintext bytes.
    CommitMetadata expected = new CommitMetadata();
    expected.addDataWithOffsetAndLength(plaintext, 0, plaintext.length);

    ByteBuf batchBuf = buildBatch(plaintext, false, handler, conf);

    // createStream passes shuffleId=1, partitionId=0, startMapIndex=0, endMapIndex=100
    ShuffleClient shuffleClient = mock(ShuffleClient.class);
    try (CelebornInputStream stream =
        createStreamWithClient(batchBuf, false, Optional.of(handler), conf, shuffleClient)) {
      assertArrayEquals(plaintext, readAll(stream));
    }

    // Verify readReducerPartitionEnd was called with the checksum over plaintext, not ciphertext.
    ArgumentCaptor<Integer> crcCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Long> bytesCaptor = ArgumentCaptor.forClass(Long.class);
    verify(shuffleClient)
        .readReducerPartitionEnd(
            anyInt(), anyInt(), anyInt(), anyInt(), crcCaptor.capture(), bytesCaptor.capture());
    assertEquals(
        "checksum must be over plaintext", expected.getChecksum(), (int) crcCaptor.getValue());
    assertEquals(
        "byte count must match plaintext length",
        expected.getBytes(),
        (long) bytesCaptor.getValue());
  }

  @Test
  public void testLargePayloadEncryptDecrypt() throws IOException, InterruptedException {
    // 128 KB payload exercises buffer-boundary handling in fillBuffer()
    byte[] plaintext = new byte[128 * 1024];
    for (int i = 0; i < plaintext.length; i++) plaintext[i] = (byte) (i % 251);
    CelebornConf conf = new CelebornConf();
    XorCryptoHandler handler = new XorCryptoHandler((byte) 0xAB);

    ByteBuf batchBuf = buildBatch(plaintext, false, handler, conf);
    try (CelebornInputStream stream = createStream(batchBuf, false, Optional.of(handler), conf)) {
      assertArrayEquals(plaintext, readAll(stream));
    }
  }
}
