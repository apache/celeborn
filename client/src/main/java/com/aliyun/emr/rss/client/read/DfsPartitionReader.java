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
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.client.ShuffleClient;
import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.client.TransportClientFactory;
import com.aliyun.emr.rss.common.network.protocol.Message;
import com.aliyun.emr.rss.common.network.protocol.OpenStream;
import com.aliyun.emr.rss.common.network.protocol.StreamHandle;
import com.aliyun.emr.rss.common.network.util.TransportConf;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;
import com.aliyun.emr.rss.common.protocol.TransportModuleConstants;
import com.aliyun.emr.rss.common.unsafe.Platform;
import com.aliyun.emr.rss.common.util.Utils;

public class DfsPartitionReader implements PartitionReader {
  private static Logger logger = LoggerFactory.getLogger(DfsPartitionReader.class);

  private final int chunkSize;
  private final int maxInFlight;
  private final LinkedBlockingQueue<ByteBuf> results;
  private final AtomicReference<IOException> exception = new AtomicReference<>();
  private boolean closed = false;
  private Thread fetchThread;
  private long offset = 0;
  private long length = -1;
  private long lastPos = 0;
  private FSDataInputStream hdfsInputStream;

  public DfsPartitionReader(RssConf conf, String shuffleKey, PartitionLocation location,
    TransportClientFactory clientFactory, int startMapIndex, int endMapIndex) throws IOException {
    chunkSize = (int) RssConf.chunkSize(conf);
    maxInFlight = RssConf.fetchChunkMaxReqsInFlight(conf);
    results = new LinkedBlockingQueue<>();

    hdfsInputStream = ShuffleClient.getHdfsFs(conf).open(
      new Path(location.getStorageInfo().getFilePath()));
    if (endMapIndex != Integer.MAX_VALUE) {
      // send rpc to the worker and get stream handler which has 12 bytes,
      // and I think there is no single shuffle file larger than 2^48(256).
      // So just store offset and length into stream handler.
      long timeoutMs = RssConf.fetchChunkTimeoutMs(conf);
      try {
        TransportClient client = clientFactory.createClient(
          location.getHost(), location.getFetchPort());
        OpenStream openBlocks = new OpenStream(shuffleKey, location.getFileName(),
          startMapIndex, endMapIndex);
        ByteBuffer response = client.sendRpcSync(openBlocks.toByteBuffer(), timeoutMs);
        StreamHandle streamHandle = (StreamHandle) Message.decode(response);
        offset = (long) Utils.convertStreamHandlerToOffsetAndLength(streamHandle.streamId,
          streamHandle.numChunks)._1;
        length = (long) Utils.convertStreamHandlerToOffsetAndLength(streamHandle.streamId,
          streamHandle.numChunks)._1;
      } catch (IOException | InterruptedException e) {
        throw new IOException("read shuffle file from hdfs failed, filePath: " +
                                location.getStorageInfo().getFilePath(), e);
      }
    } else {
      offset = 0;
      length = hdfsInputStream.available();
    }
    if (offset != 0) {
      hdfsInputStream.seek(offset);
    }
    if (length != 0) {
      lastPos = offset + length;
      fetchThread = new Thread(() -> {
        ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        try {
          byte[] header = new byte[16];
          while (!closed && offset < lastPos) {
            Arrays.fill(header, (byte) 0);
            while (results.size() >= maxInFlight) {
              Thread.sleep(50);
            }
            hdfsInputStream.readFully(offset, header);
            offset += 16;
            int bodySize = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET + 12);
            if (chunkSize - buffer.position() > bodySize + 16) {
              buffer.put(header);
              byte[] body = new byte[bodySize];
              hdfsInputStream.readFully(offset, body);
              buffer.put(body);
              offset += bodySize;
            } else {
              ByteBuf buf = Unpooled.wrappedBuffer(buffer);
              results.add(buf);

              buffer = ByteBuffer.allocate(chunkSize);
              buffer.put(header);
              byte[] body = new byte[bodySize];
              hdfsInputStream.readFully(offset, body);
              buffer.put(body);
              offset += bodySize;
            }
          }
        } catch (IOException e) {
          exception.set(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
      fetchThread.start();
    }
  }

  @Override
  public boolean hasNext() {
    return offset < lastPos && length > 0;
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
    synchronized (this) {
      closed = true;
    }
    fetchThread.interrupt();
    IOUtils.closeQuietly(hdfsInputStream, null);
    if (results.size() > 0) {
      results.forEach(res -> res.release());
    }
    results.clear();
  }
}
