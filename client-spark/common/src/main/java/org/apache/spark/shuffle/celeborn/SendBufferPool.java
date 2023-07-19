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

package org.apache.spark.shuffle.celeborn;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.celeborn.client.write.PushTask;
import org.apache.celeborn.common.util.ThreadUtils;

public class SendBufferPool {
  private static volatile SendBufferPool _instance;

  public static SendBufferPool get(int capacity, long checkInterval, long timeout) {
    if (_instance == null) {
      synchronized (SendBufferPool.class) {
        if (_instance == null) {
          _instance = new SendBufferPool(capacity, checkInterval, timeout);
        }
      }
    }
    return _instance;
  }

  private final int capacity;

  // numPartitions -> buffers
  private final LinkedList<byte[][]> buffers;
  private long lastAquireTime;
  private final LinkedList<LinkedBlockingQueue<PushTask>> pushTaskQueues;

  private ScheduledExecutorService cleaner =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("celeborn-sendbufferpool-cleaner");

  private SendBufferPool(int capacity, long checkInterval, long timeout) {
    assert capacity > 0;
    this.capacity = capacity;
    buffers = new LinkedList<>();
    pushTaskQueues = new LinkedList<>();

    lastAquireTime = System.currentTimeMillis();
    cleaner.scheduleAtFixedRate(
        () -> {
          if (System.currentTimeMillis() - lastAquireTime > timeout) {
            synchronized (this) {
              buffers.clear();
              pushTaskQueues.clear();
            }
          }
        },
        checkInterval,
        checkInterval,
        TimeUnit.MILLISECONDS);
  }

  public synchronized byte[][] acquireBuffer(int numPartitions) {
    lastAquireTime = System.currentTimeMillis();
    Iterator<byte[][]> iterator = buffers.iterator();
    while (iterator.hasNext()) {
      byte[][] candidate = iterator.next();
      if (candidate.length == numPartitions) {
        iterator.remove();
        return candidate;
      }
    }
    if (buffers.size() > 0) {
      buffers.removeFirst();
    }
    return new byte[numPartitions][];
  }

  public synchronized LinkedBlockingQueue<PushTask> acquirePushTaskQueue() {
    lastAquireTime = System.currentTimeMillis();
    if (!pushTaskQueues.isEmpty()) {
      return pushTaskQueues.removeFirst();
    }
    return null;
  }

  public synchronized void returnBuffer(byte[][] buffer) {
    if (buffers.size() == capacity) {
      buffers.removeFirst();
    }
    buffers.addLast(buffer);
  }

  public synchronized void returnPushTaskQueue(LinkedBlockingQueue<PushTask> pushTaskQueue) {
    if (pushTaskQueues.size() == capacity) {
      pushTaskQueues.removeFirst();
    }
    pushTaskQueues.addLast(pushTaskQueue);
  }
}
