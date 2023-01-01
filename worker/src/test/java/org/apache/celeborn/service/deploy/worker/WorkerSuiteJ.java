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

package org.apache.celeborn.service.deploy.worker;

import java.util.concurrent.ThreadPoolExecutor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;

public class WorkerSuiteJ {
  @Test
  public void testPooledAllocator() {
    System.out.println(Runtime.getRuntime().maxMemory());
    System.out.println(PlatformDependent.maxDirectMemory());
    System.out.println(PlatformDependent.hasUnsafe());

    PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);
    ByteBuf buf = allocator.directBuffer(1024 * 1024 * 16);
    buf.release();
  }

  @Test
  public void testMapPartitionPool() {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.worker.replicate.threads", "4");
    WorkerArguments workerArguments = new WorkerArguments(new String[] {}, conf);
    Worker worker = new Worker(conf, workerArguments);
    ThreadPoolExecutor threadPoolExecutor = worker.getMapPartitionReplicatePool("123", "123", 1);
    Assert.assertTrue(threadPoolExecutor != null);
    ThreadPoolExecutor threadPoolExecutor1_1 = worker.getMapPartitionReplicatePool("123", "123", 1);
    Assert.assertEquals(threadPoolExecutor, threadPoolExecutor1_1);
    Assert.assertEquals(worker.mapPartitionReplicatePoolHostPortMap().size(), 1);
    Assert.assertEquals(worker.mapPartitionReplicatePoolIdMap().size(), 1);
    Assert.assertEquals(worker.mapPartitionReplicateShuffleKeyMap().size(), 1);

    for (int i = 2; i < 6; i++) {
      worker.getMapPartitionReplicatePool("123", "123", i);
    }
    Assert.assertEquals(worker.mapPartitionReplicatePoolHostPortMap().size(), 5);
    Assert.assertEquals(worker.mapPartitionReplicatePoolIdMap().size(), 4);
    Assert.assertEquals(worker.mapPartitionReplicateShuffleKeyMap().size(), 1);
    Assert.assertEquals(worker.mapPartitionReplicateShuffleKeyMap().get("123").size(), 5);

    // close
    worker
        .mapPartitionReplicateShuffleKeyMap()
        .forEach(
            (p1, p2) -> {
              worker.mapPartitionReplicatePoolHostPortMap().remove("123");
              worker.mapPartitionReplicatePoolHostPortMap().remove("456");
            });
    worker.mapPartitionReplicatePoolHostPortMap().forEach((p1, p2) -> p2.shutdown());
  }
}
