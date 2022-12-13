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

package org.apache.celeborn.common.network.server.ratelimit;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.celeborn.common.util.collection.TimeSlidingHub;

public class BufferStatusHub extends TimeSlidingHub<BufferStatusHub.BufferStatusNode> {

  public static class BufferStatusNode implements TimeSlidingHub.TimeSlidingNode {

    private final AtomicLong bytesAdded;
    private final AtomicLong bytesConsumed;

    public BufferStatusNode(long bytesAdded, long bytesConsumed) {
      this.bytesAdded = new AtomicLong(bytesAdded);
      this.bytesConsumed = new AtomicLong(bytesConsumed);
    }

    public BufferStatusNode() {
      this(0, 0);
    }

    @Override
    public void combineNode(TimeSlidingNode node) {
      BufferStatusNode needToCombined = (BufferStatusNode) node;
      this.bytesAdded.addAndGet(needToCombined.bytesAdded.get());
      this.bytesConsumed.addAndGet(needToCombined.bytesConsumed.get());
    }

    @Override
    public void separateNode(TimeSlidingNode node) {
      BufferStatusNode needToCombined = (BufferStatusNode) node;
      this.bytesAdded.addAndGet(-needToCombined.bytesAdded.get());
      this.bytesConsumed.addAndGet(-needToCombined.bytesConsumed.get());
    }

    @Override
    public TimeSlidingNode clone() {
      return new BufferStatusNode(bytesAdded(), bytesConsumed());
    }

    public long bytesAdded() {
      return this.bytesAdded.get();
    }

    public long bytesConsumed() {
      return this.bytesConsumed.get();
    }

    public long bytesPendingConsumed() {
      return bytesAdded() - bytesConsumed();
    }
  }

  public BufferStatusHub(int timeWindowsInSecs) {
    super(timeWindowsInSecs);
  }

  @Override
  protected BufferStatusNode newEmptyNode() {
    return new BufferStatusNode();
  }
}
