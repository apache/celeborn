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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.identity.UserIdentifier;

public class TestRateLimitController {

  private RateLimitController controller;
  private long pendingBytes = 0L;

  @Before
  public void initialize() {
    // Make sampleTimeWindow a bit larger in case the tests run time exceed this window.
    controller =
        new RateLimitController(10, 1000, 500, 1000, 1000) {
          @Override
          public long getTotalPendingBytes() {
            return pendingBytes;
          }
        };
  }

  @After
  public void clear() {
    controller.close();
  }

  @Test
  public void testSingleUser() {
    UserIdentifier userIdentifier = new UserIdentifier("test", "celeborn");

    Assert.assertFalse(controller.isUserCongested(userIdentifier));

    controller.incrementBytes(userIdentifier, 1001);
    pendingBytes = 1001;
    Assert.assertTrue(controller.isUserCongested(userIdentifier));

    controller.decrementBytes(1001);
    pendingBytes = 0;
    Assert.assertFalse(controller.isUserCongested(userIdentifier));
  }

  @Test
  public void testMultipleUsers() {
    UserIdentifier user1 = new UserIdentifier("test", "celeborn");
    UserIdentifier user2 = new UserIdentifier("test", "spark");

    // Both users should not be congested at first
    Assert.assertFalse(controller.isUserCongested(user1));
    Assert.assertFalse(controller.isUserCongested(user2));

    // If pendingBytes exceed the high watermark, user1 produce speed > avg consume speed
    // While user2 produce speed < avg consume speed
    controller.incrementBytes(user1, 800);
    controller.incrementBytes(user2, 201);
    controller.decrementBytes(500);
    pendingBytes = 1001;
    Assert.assertTrue(controller.isUserCongested(user1));
    Assert.assertFalse(controller.isUserCongested(user2));

    // If both users higher than the avg consume speed, should congest them all.
    controller.incrementBytes(user1, 800);
    controller.incrementBytes(user2, 800);
    controller.decrementBytes(500);
    pendingBytes = 1600;
    Assert.assertTrue(controller.isUserCongested(user1));
    Assert.assertTrue(controller.isUserCongested(user2));

    // If pending bytes lower than the low watermark, should don't congest all users.
    pendingBytes = 0;
    Assert.assertFalse(controller.isUserCongested(user1));
    Assert.assertFalse(controller.isUserCongested(user2));
  }
}
