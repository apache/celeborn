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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;

public class DataBatchesSuiteJ {

  private static void addBatch(DataBatches batches, int batchId, int size) {
    byte[] body = new byte[size];
    if (size > 0) {
      body[0] = (byte) batchId;
    }
    batches.addDataBatch(null, batchId, body);
  }

  private static int[] batchIds(ArrayList<DataBatches.DataBatch> list) {
    int[] ids = new int[list.size()];
    for (int i = 0; i < list.size(); i++) {
      ids[i] = list.get(i).batchId;
    }
    return ids;
  }

  @Test
  public void requireBatchesReturnsAllAndResetsWhenRequestExceedsTotal() {
    DataBatches batches = new DataBatches();
    addBatch(batches, 1, 10);
    addBatch(batches, 2, 20);
    addBatch(batches, 3, 30);

    ArrayList<DataBatches.DataBatch> result = batches.requireBatches(100);

    assertArrayEquals(new int[] {1, 2, 3}, batchIds(result));
    assertEquals(0, batches.getTotalSize());
  }

  @Test
  public void requireBatchesReturnsAllWhenRequestEqualsTotal() {
    DataBatches batches = new DataBatches();
    addBatch(batches, 1, 10);
    addBatch(batches, 2, 20);

    ArrayList<DataBatches.DataBatch> result = batches.requireBatches(30);

    assertArrayEquals(new int[] {1, 2}, batchIds(result));
    assertEquals(0, batches.getTotalSize());
  }

  @Test
  public void requireBatchesAllPathLeavesInstanceReusable() {
    DataBatches batches = new DataBatches();
    addBatch(batches, 1, 10);

    ArrayList<DataBatches.DataBatch> first = batches.requireBatches(100);
    assertEquals(1, first.size());

    addBatch(batches, 2, 25);
    assertEquals(25, batches.getTotalSize());
    assertEquals(1, first.size());

    ArrayList<DataBatches.DataBatch> second = batches.requireBatches(100);
    assertArrayEquals(new int[] {2}, batchIds(second));
  }

  @Test
  public void requireBatchesReturnsHeadBatchesUntilRequestSatisfied() {
    DataBatches batches = new DataBatches();
    addBatch(batches, 1, 10);
    addBatch(batches, 2, 20);
    addBatch(batches, 3, 30);
    addBatch(batches, 4, 40);

    ArrayList<DataBatches.DataBatch> result = batches.requireBatches(25);

    assertArrayEquals(new int[] {1, 2}, batchIds(result));
    assertEquals(70, batches.getTotalSize());
  }

  @Test
  public void requireBatchesIncludesBoundaryBatchWhenCumulativeSizeMatchesRequest() {
    DataBatches batches = new DataBatches();
    addBatch(batches, 1, 10);
    addBatch(batches, 2, 20);
    addBatch(batches, 3, 30);

    ArrayList<DataBatches.DataBatch> result = batches.requireBatches(30);

    assertArrayEquals(new int[] {1, 2}, batchIds(result));
    assertEquals(30, batches.getTotalSize());
  }

  @Test
  public void requireBatchesPreservesOrderOfRemainingBatches() {
    DataBatches batches = new DataBatches();
    addBatch(batches, 10, 5);
    addBatch(batches, 20, 5);
    addBatch(batches, 30, 5);
    addBatch(batches, 40, 5);
    addBatch(batches, 50, 5);

    ArrayList<DataBatches.DataBatch> first = batches.requireBatches(7);
    assertArrayEquals(new int[] {10, 20}, batchIds(first));
    assertEquals(15, batches.getTotalSize());

    ArrayList<DataBatches.DataBatch> rest = batches.requireBatches(100);
    assertArrayEquals(new int[] {30, 40, 50}, batchIds(rest));
    assertEquals(0, batches.getTotalSize());
  }

  @Test
  public void requireBatchesSupportsRepeatedPartialCalls() {
    DataBatches batches = new DataBatches();
    for (int i = 0; i < 6; i++) {
      addBatch(batches, i, 10);
    }
    assertEquals(60, batches.getTotalSize());

    ArrayList<DataBatches.DataBatch> a = batches.requireBatches(15);
    assertArrayEquals(new int[] {0, 1}, batchIds(a));
    assertEquals(40, batches.getTotalSize());

    ArrayList<DataBatches.DataBatch> b = batches.requireBatches(25);
    assertArrayEquals(new int[] {2, 3, 4}, batchIds(b));
    assertEquals(10, batches.getTotalSize());

    ArrayList<DataBatches.DataBatch> c = batches.requireBatches(10);
    assertArrayEquals(new int[] {5}, batchIds(c));
    assertEquals(0, batches.getTotalSize());
  }

  @Test
  public void requireBatchesWithZeroRequestReturnsEmptyAndDoesNotMutate() {
    DataBatches batches = new DataBatches();
    addBatch(batches, 1, 10);
    addBatch(batches, 2, 20);

    ArrayList<DataBatches.DataBatch> result = batches.requireBatches(0);

    assertTrue(result.isEmpty());
    assertEquals(30, batches.getTotalSize());

    // Underlying batches still intact and in original order.
    ArrayList<DataBatches.DataBatch> all = batches.requireBatches(100);
    assertArrayEquals(new int[] {1, 2}, batchIds(all));
  }

  @Test
  public void requireBatchesOnEmptyReturnsEmpty() {
    DataBatches batches = new DataBatches();

    ArrayList<DataBatches.DataBatch> result = batches.requireBatches(0);

    assertTrue(result.isEmpty());
    assertEquals(0, batches.getTotalSize());
  }

  @Test
  public void requireBatchesReturnedBatchesAreIndependentOfFutureAdds() {
    DataBatches batches = new DataBatches();
    addBatch(batches, 1, 10);
    addBatch(batches, 2, 20);
    addBatch(batches, 3, 30);

    ArrayList<DataBatches.DataBatch> result = batches.requireBatches(15);
    assertArrayEquals(new int[] {1, 2}, batchIds(result));

    addBatch(batches, 4, 40);
    assertArrayEquals(new int[] {1, 2}, batchIds(result));
    assertEquals(70, batches.getTotalSize());
  }
}
