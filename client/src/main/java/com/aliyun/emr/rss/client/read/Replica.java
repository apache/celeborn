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

import com.google.common.annotations.VisibleForTesting;

import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.client.TransportClientFactory;
import com.aliyun.emr.rss.common.network.protocol.Message;
import com.aliyun.emr.rss.common.network.protocol.OpenStream;
import com.aliyun.emr.rss.common.network.protocol.StreamHandle;
import com.aliyun.emr.rss.common.protocol.PartitionLocation;

class Replica {
  private final long timeoutMs;
  private final String shuffleKey;
  private final PartitionLocation location;
  private final TransportClientFactory clientFactory;

  private StreamHandle streamHandle;
  private TransportClient client;
  private final int startMapIndex;
  private final int endMapIndex;

  Replica(
      long timeoutMs,
      String shuffleKey,
      PartitionLocation location,
      TransportClientFactory clientFactory,
      int startMapIndex,
      int endMapIndex) {
    this.timeoutMs = timeoutMs;
    this.shuffleKey = shuffleKey;
    this.location = location;
    this.clientFactory = clientFactory;
    this.startMapIndex = startMapIndex;
    this.endMapIndex = endMapIndex;
  }

  public synchronized TransportClient getOrOpenStream() throws IOException, InterruptedException {
    if (client == null || !client.isActive()) {
      client = clientFactory.createClient(location.getHost(), location.getFetchPort());
    }
    if (streamHandle == null) {
      OpenStream openBlocks =
          new OpenStream(shuffleKey, location.getFileName(), startMapIndex, endMapIndex);
      ByteBuffer response = client.sendRpcSync(openBlocks.toByteBuffer(), timeoutMs);
      streamHandle = (StreamHandle) Message.decode(response);
    }
    return client;
  }

  public long getStreamId() {
    return streamHandle.streamId;
  }

  public int getNumChunks() {
    return streamHandle.numChunks;
  }

  @Override
  public String toString() {
    String shufflePartition =
        String.format("%s:%d %s", location.getHost(), location.getFetchPort(), shuffleKey);
    if (startMapIndex == 0 && endMapIndex == Integer.MAX_VALUE) {
      return shufflePartition;
    } else {
      return String.format("%s[%d,%d)", shufflePartition, startMapIndex, endMapIndex);
    }
  }

  @VisibleForTesting
  PartitionLocation getLocation() {
    return location;
  }
}
