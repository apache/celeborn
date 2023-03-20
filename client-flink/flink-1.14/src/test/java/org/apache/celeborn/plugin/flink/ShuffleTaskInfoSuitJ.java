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

package org.apache.celeborn.plugin.flink;

import org.junit.Assert;
import org.junit.Test;

public class ShuffleTaskInfoSuitJ {
  @Test
  public void testEncode() {
    ShuffleTaskInfo shuffleTaskInfo = new ShuffleTaskInfo();
    int encodeShuffleId = shuffleTaskInfo.getShuffleId("shuffleId");
    assert (encodeShuffleId == 0);

    // another shuffle
    int encodeShuffleId1 = shuffleTaskInfo.getShuffleId("shuffleId1");
    assert (encodeShuffleId1 == 1);

    int encodeShuffleId0 = shuffleTaskInfo.getShuffleId("shuffleId");
    Assert.assertEquals(encodeShuffleId0, 0);

    int encodeAttemptId011 = shuffleTaskInfo.getAttemptId("shuffleId", 1, "attempt1");
    int encodeAttemptId112 = shuffleTaskInfo.getAttemptId("shuffleId1", 1, "attempt2");
    int encodeAttemptId021 = shuffleTaskInfo.getAttemptId("shuffleId", 2, "attempt1");
    int encodeAttemptId012 = shuffleTaskInfo.getAttemptId("shuffleId", 1, "attempt2");
    Assert.assertEquals(encodeAttemptId011, 0);
    Assert.assertEquals(encodeAttemptId112, 0);
    Assert.assertEquals(encodeAttemptId021, 0);
    Assert.assertEquals(encodeAttemptId012, 1);

    // remove shuffleId and reEncode
    shuffleTaskInfo.removeExpiredShuffle(encodeShuffleId);
    int encodeShuffleIdNew = shuffleTaskInfo.getShuffleId("shuffleId");
    Assert.assertEquals(encodeShuffleIdNew, 2);
  }

  @Test
  public void testRemoveNonExistShuffl() {
    ShuffleTaskInfo shuffleTaskInfo = new ShuffleTaskInfo();
    // remove none exist shuffle
    shuffleTaskInfo.removeExpiredShuffle(0);
  }
}
