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

package org.apache.celeborn.common.network.server;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferStreamManager {
  private static final Logger logger = LoggerFactory.getLogger(BufferStreamManager.class);
  private final AtomicLong nextStreamId;
  protected final ConcurrentHashMap<Long, StreamStat> streams;

  protected class StreamStat {
    private Channel associatedChannel;
    private int bufferSize;

    public StreamStat(Channel associatedChannel, int bufferSize) {
      this.associatedChannel = associatedChannel;
      this.bufferSize = bufferSize;
    }

    public Channel getAssociatedChannel() {
      return associatedChannel;
    }

    public int getBufferSize() {
      return bufferSize;
    }
  }

  public BufferStreamManager() {
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = new ConcurrentHashMap<>();
  }

  public long registerStream(Channel channel, int bufferSize) {
    long streamId = nextStreamId.getAndIncrement();
    streams.put(streamId, new StreamStat(channel, bufferSize));
    return streamId;
  }

  public void addCredit(int numCredit, long streamId) {}

  public void connectionTerminated(Channel channel) {
    for (Map.Entry<Long, StreamStat> entry : streams.entrySet()) {
      if (entry.getValue().getAssociatedChannel().equals(channel)) {
        streams.remove(entry.getKey());
      }
    }
  }
}
