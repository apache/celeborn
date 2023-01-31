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

package org.apache.celeborn.client.write;

import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;

public class SlowStartPushStrategyTest {

  private final CelebornConf conf = new CelebornConf();

  @Test
  public void testSleepTime() {
    conf.set("celeborn.push.maxReqsInFlight", "32");
    conf.set("celeborn.push.limit.strategy", "slowstart");
    conf.set("celeborn.push.slowStart.maxSleepTime", "4s");
    SlowStartPushStrategy strategy = (SlowStartPushStrategy) PushStrategy.getStrategy(conf);

    // If the currentReq is 0, not throw error
    strategy.getSleepTime(0);

    // If the currentReq is 1, should sleep 1940 ms
    Assert.assertEquals(1940, strategy.getSleepTime(1));

    // Keep congest the requests, should increase the sleep time
    strategy.onCongestControl();
    Assert.assertEquals(2940, strategy.getSleepTime(1));
    strategy.onCongestControl();
    Assert.assertEquals(3940, strategy.getSleepTime(1));

    // Cannot exceed the max sleep time
    strategy.onCongestControl();
    Assert.assertEquals(4000, strategy.getSleepTime(1));

    // If not congested, should not increase the sleep time
    strategy.onSuccess();
    Assert.assertEquals(1940, strategy.getSleepTime(1));

    // If the currentReq is 16, should sleep 1040 ms
    Assert.assertEquals(1040, strategy.getSleepTime(16));

    // If the currentReq is 32, should sleep 0 ms
    Assert.assertEquals(0, strategy.getSleepTime(32));
  }

  @Test
  public void testCongestStrategy() {
    conf.set("celeborn.push.maxReqsInFlight", "5");
    conf.set("celeborn.push.limit.strategy", "slowstart");
    conf.set("celeborn.push.slowStart.maxSleepTime", "4s");
    SlowStartPushStrategy strategy = (SlowStartPushStrategy) PushStrategy.getStrategy(conf);

    // Slow start, should exponentially increase the currentReq
    strategy.onSuccess();
    Assert.assertEquals(1, strategy.getCurrentMaxReqsInFlight());
    strategy.onSuccess();
    Assert.assertEquals(2, strategy.getCurrentMaxReqsInFlight());
    strategy.onSuccess();
    strategy.onSuccess();
    Assert.assertEquals(4, strategy.getCurrentMaxReqsInFlight());
    strategy.onSuccess();
    strategy.onSuccess();
    strategy.onSuccess();
    strategy.onSuccess();
    Assert.assertEquals(5, strategy.getCurrentMaxReqsInFlight());

    // Will linearly increase the currentReq if meet the maxReqsInFlight
    strategy.onSuccess();
    strategy.onSuccess();
    strategy.onSuccess();
    strategy.onSuccess();
    strategy.onSuccess();
    Assert.assertEquals(6, strategy.getCurrentMaxReqsInFlight());

    // Congest controlled, should half the currentReq
    strategy.onCongestControl();
    Assert.assertEquals(3, strategy.getCurrentMaxReqsInFlight());
    strategy.onCongestControl();
    Assert.assertEquals(1, strategy.getCurrentMaxReqsInFlight());

    // Cannot lower than 1
    strategy.onCongestControl();
    Assert.assertEquals(1, strategy.getCurrentMaxReqsInFlight());
  }
}
