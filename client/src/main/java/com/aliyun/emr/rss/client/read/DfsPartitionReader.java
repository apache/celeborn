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

package com.aliyun.emr.rss.client.read;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.aliyun.emr.rss.client.ShuffleClient;
import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.client.TransportClientFactory;
import com.aliyun.emr.rss.common.network.protocol.Message;
import com.aliyun.emr.rss.common.network.protocol.OpenStream;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;
import com.aliyun.emr.rss.common.util.Utils;
import com.aliyun.emr.rss.common.utils.ShuffleBlockInfoUtils;

public class DfsPartitionReader implements PartitionReader {
  private final int chunkSize;
  private final int maxInFlight;
  private final LinkedBlockingQueue<ByteBuf> results;
  private final AtomicReference<IOException> exception = new AtomicReference<>();
  private volatile boolean closed = false;
  private Thread fetchThread;
  private final FSDataInputStream hdfsInputStream;
  private int numChunks = 0;
  private final AtomicInteger currentChunkIndex = new AtomicInteger(0);

  public DfsPartitionReader(
      RssConf conf,
      String shuffleKey,
      PartitionLocation location,
      TransportClientFactory clientFactory,
      int startMapIndex,
      int endMapIndex)
      throws IOException {
    chunkSize = (int) RssConf.chunkSize(conf);
    maxInFlight = RssConf.fetchChunkMaxReqsInFlight(conf);
    results = new LinkedBlockingQueue<>();

    final List<Long> chunkOffsets = new ArrayList<>();
    if (endMapIndex != Integer.MAX_VALUE) {
      long timeoutMs = RssConf.fetchChunkTimeoutMs(conf);
      try {
        TransportClient client =
            clientFactory.createClient(location.getHost(), location.getFetchPort());
        OpenStream openBlocks =
            new OpenStream(shuffleKey, location.getFileName(), startMapIndex, endMapIndex);
        ByteBuffer response = client.sendRpcSync(openBlocks.toByteBuffer(), timeoutMs);
        Message.decode(response);
        // Parse this message to ensure sort is done.
      } catch (IOException | InterruptedException e) {
        throw new IOException(
            "read shuffle file from hdfs failed, filePath: "
                + location.getStorageInfo().getFilePath(),
            e);
      }
      hdfsInputStream =
          ShuffleClient.getHdfsFs(conf)
              .open(new Path(Utils.getSortedFilePath(location.getStorageInfo().getFilePath())));
      chunkOffsets.addAll(
          getChunkOffsetsFromSortedIndex(conf, location, startMapIndex, endMapIndex));
    } else {
      hdfsInputStream =
          ShuffleClient.getHdfsFs(conf).open(new Path(location.getStorageInfo().getFilePath()));
      chunkOffsets.addAll(getChunkOffsetsFromUnsortedIndex(conf, location));
    }
    if (chunkOffsets.size() > 1) {
      numChunks = chunkOffsets.size() - 1;
      fetchThread =
          new Thread(
              () -> {
                try {
                  while (!closed && currentChunkIndex.get() < numChunks) {
                    while (results.size() >= maxInFlight) {
                      Thread.sleep(50);
                    }
                    long offset = chunkOffsets.get(currentChunkIndex.get());
                    long length = chunkOffsets.get(currentChunkIndex.get() + 1) - offset;
                    ByteBuffer buffer = ByteBuffer.allocate((int) length);
                    hdfsInputStream.readFully(offset, buffer);
                    results.add(Unpooled.wrappedBuffer(buffer));
                    currentChunkIndex.incrementAndGet();
                  }
                } catch (IOException e) {
                  exception.set(e);
                } catch (InterruptedException e) {
                  // cancel a task for speculative, ignore this exception
                }
              });
      fetchThread.start();
    }
  }

  private List<Long> getChunkOffsetsFromUnsortedIndex(RssConf conf, PartitionLocation location)
      throws IOException {
    FSDataInputStream indexInputStream =
        ShuffleClient.getHdfsFs(conf)
            .open(new Path(Utils.getIndexFilePath(location.getStorageInfo().getFilePath())));
    List<Long> offsets = new ArrayList<>();
    int offsetCount = indexInputStream.readInt();
    for (int i = 0; i < offsetCount; i++) {
      offsets.add(indexInputStream.readLong());
    }
    indexInputStream.close();
    return offsets;
  }

  private List<Long> getChunkOffsetsFromSortedIndex(
      RssConf conf, PartitionLocation location, int startMapIndex, int endMapIndex)
      throws IOException {
    String indexPath = Utils.getIndexFilePath(location.getStorageInfo().getFilePath());
    FSDataInputStream indexInputStream = ShuffleClient.getHdfsFs(conf).open(new Path(indexPath));
    long indexSize = ShuffleClient.getHdfsFs(conf).getFileStatus(new Path(indexPath)).getLen();
    // Index size won't be large, so it's safe to do the conversion.
    ByteBuffer indexBuffer = ByteBuffer.allocate((int) indexSize);
    indexInputStream.readFully(0L, indexBuffer);
    List<Long> offsets =
        new ArrayList<>(
            ShuffleBlockInfoUtils.getChunkOffsetsFromShuffleBlockInfos(
                startMapIndex,
                endMapIndex,
                chunkSize,
                ShuffleBlockInfoUtils.parseShuffleBlockInfosFromByteBuffer(indexBuffer)));
    indexInputStream.close();
    return offsets;
  }

  @Override
  public boolean hasNext() {
    return currentChunkIndex.get() < numChunks;
  }

  @Override
  public ByteBuf next() throws IOException {
    checkException();
    ByteBuf chunk = null;
    try {
      while (chunk == null) {
        checkException();
        chunk = results.poll(500, TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IOException ioe = new IOException(e);
      exception.set(ioe);
      throw ioe;
    }
    return chunk;
  }

  private void checkException() throws IOException {
    IOException e = exception.get();
    if (e != null) {
      throw e;
    }
  }

  @Override
  public void close() {
    closed = true;
    fetchThread.interrupt();
    IOUtils.closeQuietly(hdfsInputStream, null);
    if (results.size() > 0) {
      results.forEach(ReferenceCounted::release);
    }
    results.clear();
  }
}
