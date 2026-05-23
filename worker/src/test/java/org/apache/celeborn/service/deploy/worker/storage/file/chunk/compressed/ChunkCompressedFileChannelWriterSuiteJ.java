package org.apache.celeborn.service.deploy.worker.storage.file.chunk.compressed;

import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

import com.github.luben.zstd.Zstd;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;
import org.apache.celeborn.common.network.buffer.FileChunkBuffers;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.StorageInfo;

public class ChunkCompressedFileChannelWriterSuiteJ {
  @Test
  public void testChunkCompressedFileChannelWriter() throws IOException {
    File file = File.createTempFile("test_data_writer", "tmp");
    DiskFileInfo diskFileInfo =
        new DiskFileInfo(
            new UserIdentifier("t1", "u1"),
            true,
            new ReduceFileMeta(new ArrayList<>(Arrays.asList(0L)), 100),
            file.getAbsolutePath(),
            StorageInfo.Type.HDD,
            true);
    ChunkCompressedFileChannelWriter writer = new ChunkCompressedFileChannelWriter(diskFileInfo, 8 * 1024 * 1024);

    ByteBuf buf = Unpooled.wrappedBuffer("hello world1".getBytes(StandardCharsets.UTF_8));
    ByteBuf buf2 = Unpooled.wrappedBuffer("hello world2".getBytes(StandardCharsets.UTF_8));
    ByteBuf buf3 = Unpooled.wrappedBuffer("hello world3".getBytes(StandardCharsets.UTF_8));
    ByteBuf buf4 = Unpooled.wrappedBuffer("hello world4".getBytes(StandardCharsets.UTF_8));

    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
    compositeByteBuf.addComponent(true, buf);
    compositeByteBuf.addComponent(true, buf2);
    compositeByteBuf.addComponent(true, buf3);
    compositeByteBuf.addComponent(true, buf4);

    writer.write(compositeByteBuf, true);

    writer.close(true);

    // Read chunks directly from file
    TransportConf transportConf = mock(TransportConf.class);
    FileChunkBuffers buffers = new FileChunkBuffers(diskFileInfo, transportConf);
    int numChunks = buffers.numChunks();
    String expectedContent = "hello world1hello world2hello world3hello world4";
    ByteArrayOutputStream decompressedBytes = new ByteArrayOutputStream();
    for (int i = 0; i < numChunks; i++) {
      // Read the chunk
      ByteBuffer internalBuf = buffers.chunk(i, 0, Integer.MAX_VALUE).nioByteBuffer();
      byte[] compressedBytes = new byte[internalBuf.remaining()];
      internalBuf.get(compressedBytes);
      Assert.assertTrue("Chunk " + i + " should be non-empty", compressedBytes.length > 0);

      long decompressedSize = Zstd.decompressedSize(compressedBytes);
      Assert.assertTrue(
          "Chunk " + i + " has invalid decompressed size",
          decompressedSize > 0 && decompressedSize <= Integer.MAX_VALUE);
      byte[] decompressedChunk = new byte[(int) decompressedSize];
      long actualDecompressedSize =
          Zstd.decompressByteArray(
              decompressedChunk,
              0,
              decompressedChunk.length,
              compressedBytes,
              0,
              compressedBytes.length);
      Assert.assertFalse(
          "Chunk " + i + " failed to decompress", Zstd.isError(actualDecompressedSize));
      Assert.assertEquals(
          "Chunk " + i + " decompressed size mismatch",
          decompressedChunk.length,
          (int) actualDecompressedSize);
      decompressedBytes.write(decompressedChunk);
    }
    Assert.assertEquals(expectedContent, decompressedBytes.toString(StandardCharsets.UTF_8.name()));
  }
}
