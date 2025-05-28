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

package org.apache.celeborn.common.write;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;

public class LocationPushFailedBatchesSuiteJ {

  @Test
  public void equalsReturnsTrueForIdenticalBatches() {
    LocationPushFailedBatches batch1 = new LocationPushFailedBatches();
    batch1.addFailedBatch(1, 2, 3);
    LocationPushFailedBatches batch2 = new LocationPushFailedBatches();
    batch2.addFailedBatch(1, 2, 3);
    Assert.assertEquals(batch1, batch2);
  }

  @Test
  public void equalsReturnsFalseForDifferentBatches() {
    LocationPushFailedBatches batch1 = new LocationPushFailedBatches();
    batch1.addFailedBatch(1, 2, 3);
    LocationPushFailedBatches batch2 = new LocationPushFailedBatches();
    batch2.addFailedBatch(4, 5, 6);
    Assert.assertNotEquals(batch1, batch2);
  }

  @Test
  public void hashCodeDiffersForDifferentBatches() {
    LocationPushFailedBatches batch1 = new LocationPushFailedBatches();
    batch1.addFailedBatch(1, 2, 3);
    LocationPushFailedBatches batch2 = new LocationPushFailedBatches();
    batch2.addFailedBatch(4, 5, 6);
    Assert.assertNotEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void hashCodeSameForIdenticalBatches() {
    LocationPushFailedBatches batch1 = new LocationPushFailedBatches();
    batch1.addFailedBatch(1, 2, 3);
    LocationPushFailedBatches batch2 = new LocationPushFailedBatches();
    batch2.addFailedBatch(1, 2, 3);
    Assert.assertEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void hashCodeIsConsistent() {
    LocationPushFailedBatches batch = new LocationPushFailedBatches();
    batch.addFailedBatch(1, 2, 3);
    int hashCode1 = batch.hashCode();
    int hashCode2 = batch.hashCode();
    Assert.assertEquals(hashCode1, hashCode2);
  }

  @Test
  public void toStringReturnsExpectedFormat() {
    LocationPushFailedBatches batch = new LocationPushFailedBatches();
    batch.addFailedBatch(1, 2, 3);
    String expected =
        "LocationPushFailedBatches{failedBatches=failed attemptKey:1-2 fail batch Ids:3}";
    Assert.assertEquals(expected, batch.toString());
  }

  @Test
  public void hashCodeAndEqualsWorkInSet() {
    Set<LocationPushFailedBatches> set = new HashSet<>();
    LocationPushFailedBatches batch1 = new LocationPushFailedBatches();
    batch1.addFailedBatch(1, 2, 3);
    LocationPushFailedBatches batch2 = new LocationPushFailedBatches();
    batch2.addFailedBatch(1, 2, 3);
    set.add(batch1);
    assertTrue(set.contains(batch2));
  }

  @Test
  public void concurrentAddAndGetShouldNotConflict() throws InterruptedException {
    LocationPushFailedBatches batches = new LocationPushFailedBatches();
    ExecutorService executor = Executors.newFixedThreadPool(4);

    int totalFailedBatches = 1000;
    for (int i = 0; i < totalFailedBatches; i++) {
      final int tIdx = i;
      executor.submit(() -> batches.addFailedBatch(tIdx % 10, tIdx % 5, tIdx));
    }
    executor.shutdown();
    executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
    assertTrue(!batches.getFailedBatches().isEmpty());
    assertEquals(
        totalFailedBatches, batches.getFailedBatches().values().stream().mapToInt(Set::size).sum());
  }
}
