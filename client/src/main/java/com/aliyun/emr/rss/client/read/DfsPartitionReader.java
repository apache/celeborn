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

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.client.TransportClientFactory;
import com.aliyun.emr.rss.common.network.protocol.Message;
import com.aliyun.emr.rss.common.network.protocol.OpenStream;
import com.aliyun.emr.rss.common.network.protocol.StreamHandle;
import com.aliyun.emr.rss.common.network.util.TransportConf;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;
import com.aliyun.emr.rss.common.protocol.TransportModuleConstants;
import com.aliyun.emr.rss.common.util.Utils;

public class DfsPartitionReader implements PartitionReader {
  private static Logger logger = LoggerFactory.getLogger(DfsPartitionReader.class);

  private final int chunkSize;

  private final long offset = 0;
  private final long length = 0;

  public DfsPartitionReader(RssConf conf, String shuffleKey, PartitionLocation location,
    TransportClientFactory clientFactory, int startMapIndex, int endMapIndex) {
    chunkSize = (int) RssConf.chunkSize(conf);

    if (endMapIndex == Integer.MAX_VALUE) {
      // read directly from hdfs

    } else {
      // send rpc to the worker and get stream handler which has 12 bytes,
      // and I think there is no single shuffle file larger than 2^48(256).
      // So just store offset and length into stream handler.
      TransportConf transportConf = Utils.fromRssConf(conf, TransportModuleConstants.DATA_MODULE, 0);
      int retryWaitMs = transportConf.ioRetryWaitTimeMs();
      long timeoutMs = RssConf.fetchChunkTimeoutMs(conf);
      try {
        TransportClient client = clientFactory.createClient(location.getHost(), location.getFetchPort());
        OpenStream openBlocks = new OpenStream(shuffleKey, location.getFileName(),
          startMapIndex, endMapIndex);
        ByteBuffer response = client.sendRpcSync(openBlocks.toByteBuffer(), timeoutMs);
        StreamHandle streamHandle = (StreamHandle) Message.decode(response);

      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean hasData() {
    return false;
  }

  @Override
  public ByteBuf next() throws IOException {
    return null;
  }

  @Override
  public void close() {

  }
}
