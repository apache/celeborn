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
package org.apache.celeborn.service.deploy.worker.memory;

import java.util.Random;

import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;

public class ReadBufferDispatcherSuiteJ {

  @Test
  public void testReadBuffer() throws InterruptedException {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.network.memory.allocator.allowCache", "false");
    MemoryManager.initialize(conf);
    ReadBufferDispatcher readBufferDispatcher =
        new ReadBufferDispatcher(MemoryManager.instance(), conf);

    int threadNum = 16;
    Thread thread[] = new Thread[threadNum];

    for (int i = 0; i < 16; i++) {
      thread[i] =
          new Thread(
              () -> {
                for (int j = 0; j < 1000; j++) {
                  long start = System.currentTimeMillis();
                  readBufferDispatcher.addBufferRequest(
                      new ReadBufferRequest(
                          new Random().nextInt(200) + 1,
                          32 * 1024,
                          (allocatedBuffers, throwable) -> {
                            long apply = System.currentTimeMillis();
                            for (ByteBuf allocatedBuffer : allocatedBuffers) {
                              readBufferDispatcher.recycle(allocatedBuffer);
                            }
                            long release = System.currentTimeMillis();
                            System.out.println(
                                "" + (apply - start) + "ms, release" + (release - apply));
                          }));
                }
              });
      thread[i].start();
    }

    for (int i = 0; i < thread.length; i++) {
      thread[i].join();
    }
  }

  @Test
  public void testCase2() throws InterruptedException {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.network.memory.allocator.allowCache", "false");
    MemoryManager.initialize(conf);
    ReadBufferDispatcher readBufferDispatcher =
        new ReadBufferDispatcher(MemoryManager.instance(), conf);

    readBufferDispatcher.addBufferRequest(
        new ReadBufferRequest(
            new Random().nextInt(200) + 1,
            32 * 1024,
            (allocatedBuffers, throwable) -> {
              for (ByteBuf allocatedBuffer : allocatedBuffers) {
                readBufferDispatcher.recycle(allocatedBuffer);
              }
            }));

    readBufferDispatcher.addBufferRequest(
        new ReadBufferRequest(
            new Random().nextInt(200) + 1,
            32 * 1024,
            (allocatedBuffers, throwable) -> {
              for (ByteBuf allocatedBuffer : allocatedBuffers) {
                readBufferDispatcher.recycle(allocatedBuffer);
              }
            }));

    readBufferDispatcher.addBufferRequest(
        new ReadBufferRequest(
            new Random().nextInt(200) + 1,
            32 * 1024,
            (allocatedBuffers, throwable) -> {
              for (ByteBuf allocatedBuffer : allocatedBuffers) {
                readBufferDispatcher.recycle(allocatedBuffer);
              }
            }));

    // wait buffer allocation complete
    Thread.sleep(1000l);

    Assert.assertTrue(readBufferDispatcher.idleBuffersLength() > 0);
  }

  @Test
  public void testCase3() throws InterruptedException {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.network.memory.allocator.allowCache", "false");
    MemoryManager.initialize(conf);
    ReadBufferDispatcher readBufferDispatcher =
        new ReadBufferDispatcher(MemoryManager.instance(), conf);

    readBufferDispatcher.addBufferRequest(
        new ReadBufferRequest(
            new Random().nextInt(200) + 1,
            32 * 1024,
            (allocatedBuffers, throwable) -> {
              for (ByteBuf allocatedBuffer : allocatedBuffers) {
                readBufferDispatcher.recycle(allocatedBuffer);
              }
            }));

    readBufferDispatcher.addBufferRequest(
        new ReadBufferRequest(
            new Random().nextInt(200) + 1,
            32 * 1024,
            (allocatedBuffers, throwable) -> {
              for (ByteBuf allocatedBuffer : allocatedBuffers) {
                readBufferDispatcher.recycle(allocatedBuffer);
              }
            }));

    readBufferDispatcher.addBufferRequest(
        new ReadBufferRequest(
            new Random().nextInt(200) + 1,
            32 * 1024,
            (allocatedBuffers, throwable) -> {
              for (ByteBuf allocatedBuffer : allocatedBuffers) {
                readBufferDispatcher.recycle(allocatedBuffer);
              }
            }));

    // wait buffer allocation complete
    Thread.sleep(1000l);

    readBufferDispatcher.onTrim();

    Thread.sleep(1000l);

    Assert.assertTrue(readBufferDispatcher.idleBuffersLength() == 0);
  }
}
