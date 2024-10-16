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

package org.apache.celeborn.service.deploy.worker.congestcontrol;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

public class TestCongestionController {

  private CongestionController controller;
  private WorkerSource source = new WorkerSource(new CelebornConf());

  private long pendingBytes = 0L;
  private final long userInactiveTimeMills = 2000L;
  private final long checkIntervalTimeMills = Integer.MAX_VALUE;

  @Before
  public void initialize() {
    // Make sampleTimeWindow a bit larger in case the tests run time exceed this window.
    controller =
        new CongestionController(
            source,
            10,
            1000,
            500,
            20000,
            10000,
            20000,
            10000,
            userInactiveTimeMills,
            checkIntervalTimeMills) {
          @Override
          public long getTotalPendingBytes() {
            return pendingBytes;
          }

          @Override
          public void trimMemoryUsage() {
            // No op
          }
        };
  }

  @After
  public void clear() {
    controller.close();
    source.destroy();
  }

  @Test
  public void testSingleUser() {
    UserIdentifier userIdentifier = new UserIdentifier("test", "celeborn");
    UserCongestionControlContext userCongestionControlContext =
        controller.getUserCongestionContext(userIdentifier);
    Assert.assertFalse(controller.isUserCongested(userCongestionControlContext));

    produceBytes(controller, userIdentifier, 1001);
    pendingBytes = 1001;
    controller.checkCongestion();
    Assert.assertTrue(controller.isUserCongested(userCongestionControlContext));

    controller.getProducedBufferStatusHub().add(new BufferStatusHub.BufferStatusNode(1001));
    pendingBytes = 0;
    controller.checkCongestion();
    Assert.assertFalse(controller.isUserCongested(userCongestionControlContext));
    clearBufferStatus(controller);
  }

  @Test
  public void testMultipleUsers() {
    UserIdentifier user1 = new UserIdentifier("test", "celeborn");
    UserIdentifier user2 = new UserIdentifier("test", "spark");

    UserCongestionControlContext context1 = controller.getUserCongestionContext(user1);
    UserCongestionControlContext context2 = controller.getUserCongestionContext(user2);

    // Both users should not be congested at first
    Assert.assertFalse(controller.isUserCongested(context1));
    Assert.assertFalse(controller.isUserCongested(context2));

    // If pendingBytes exceed the high watermark, user1 produce speed > avg produce speed
    // While user2 produce speed < avg produce speed
    produceBytes(controller, user1, 800);
    produceBytes(controller, user2, 201);
    controller.getProducedBufferStatusHub().add(new BufferStatusHub.BufferStatusNode(500));
    pendingBytes = 1001;
    controller.checkCongestion();
    Assert.assertTrue(controller.isUserCongested(context1));
    Assert.assertFalse(controller.isUserCongested(context2));

    // If both users higher than the avg produce speed, should congest them all.
    produceBytes(controller, user1, 800);
    produceBytes(controller, user2, 800);
    controller.getProducedBufferStatusHub().add(new BufferStatusHub.BufferStatusNode(500));
    pendingBytes = 1600;
    controller.checkCongestion();
    Assert.assertTrue(controller.isUserCongested(context1));
    Assert.assertTrue(controller.isUserCongested(context2));

    // If pending bytes lower than the low watermark, should don't congest all users.
    pendingBytes = 0;
    controller.checkCongestion();
    Assert.assertFalse(controller.isUserCongested(context1));
    Assert.assertFalse(controller.isUserCongested(context2));
    clearBufferStatus(controller);
  }

  @Test
  public void testUserMetrics() throws InterruptedException {
    UserIdentifier user = new UserIdentifier("test", "celeborn");
    UserCongestionControlContext context = controller.getUserCongestionContext(user);

    Assert.assertFalse(controller.isUserCongested(context));
    produceBytes(controller, user, 800);

    Assert.assertTrue(source.gaugeExists(WorkerSource.USER_PRODUCE_SPEED(), user.toMap()));

    Thread.sleep(userInactiveTimeMills * 2 + 1);

    Assert.assertFalse(source.gaugeExists(WorkerSource.USER_PRODUCE_SPEED(), user.toMap()));
    clearBufferStatus(controller);
  }

  public void produceBytes(
      CongestionController controller, UserIdentifier userIdentifier, long numBytes) {
    controller
        .getUserBuffer(userIdentifier)
        .updateInfo(System.currentTimeMillis(), new BufferStatusHub.BufferStatusNode(numBytes));
  }

  @Test
  public void testUserLevelTrafficQuota() throws InterruptedException {
    CongestionController controller1 =
        new CongestionController(
            source, 10, 100000, 50000, 500, 400, 1200, 1000, 120L * 1000, checkIntervalTimeMills) {
          @Override
          public long getTotalPendingBytes() {
            return 0;
          }

          @Override
          public void trimMemoryUsage() {
            // No op
          }
        };

    UserIdentifier user1 = new UserIdentifier("test1", "celeborn");
    UserCongestionControlContext context1 = controller1.getUserCongestionContext(user1);
    Assert.assertFalse(controller1.isUserCongested(context1));
    produceBytes(controller1, user1, 600);
    controller1.getProducedBufferStatusHub().add(new BufferStatusHub.BufferStatusNode(600));
    Assert.assertTrue(controller1.isUserCongested(context1));
    Thread.sleep(1001);
    produceBytes(controller1, user1, 100);
    // user1 produce speed is 350mb/s
    Assert.assertFalse(controller1.isUserCongested(context1));

    UserIdentifier user2 = new UserIdentifier("test2", "celeborn");
    UserCongestionControlContext context2 = controller1.getUserCongestionContext(user2);
    Assert.assertFalse(controller1.isUserCongested(context2));
    produceBytes(controller1, user2, 400);
    // user2 produce speed is 400mb/s
    Assert.assertFalse(controller1.isUserCongested(context2));

    UserIdentifier user3 = new UserIdentifier("test3", "celeborn");
    UserCongestionControlContext context3 = controller1.getUserCongestionContext(user3);
    Assert.assertFalse(controller1.isUserCongested(context3));
    produceBytes(controller1, user3, 400);
    // user produce speed is 400mb/s
    Assert.assertFalse(controller1.isUserCongested(context3));

    produceBytes(controller1, user1, 400);
    controller1.getProducedBufferStatusHub().add(new BufferStatusHub.BufferStatusNode(1300));
    controller1.checkCongestion();
    // user1 -> 550mb/s, user2 -> 400mb/s, user3 -> 400mb/s, avg consume 316mb/s
    Assert.assertTrue(controller1.isUserCongested(context1));
    Assert.assertFalse(controller1.isUserCongested(context2));
    Assert.assertFalse(controller1.isUserCongested(context3));

    Thread.sleep(1001);
    produceBytes(controller1, user1, 10);
    produceBytes(controller1, user2, 50);
    produceBytes(controller1, user3, 50);
    controller1.getProducedBufferStatusHub().add(new BufferStatusHub.BufferStatusNode(110));
    controller1.checkCongestion();
    Assert.assertFalse(controller1.isUserCongested(context1));
    Assert.assertFalse(controller1.isUserCongested(context2));
    Assert.assertFalse(controller1.isUserCongested(context3));
    controller1.close();
  }

  @Test
  public void testWorkerLevelTrafficQuota() throws InterruptedException {
    CongestionController controller1 =
        new CongestionController(
            source, 10, 100000, 50000, 500, 400, 800, 700, 120 * 1000, checkIntervalTimeMills) {
          @Override
          public long getTotalPendingBytes() {
            return 0;
          }

          @Override
          public void trimMemoryUsage() {
            // No op
          }
        };

    UserIdentifier user1 = new UserIdentifier("test1", "celeborn");
    UserCongestionControlContext context1 = controller1.getUserCongestionContext(user1);
    Assert.assertFalse(controller1.isUserCongested(context1));
    produceBytes(controller1, user1, 500);
    controller1.getProducedBufferStatusHub().add(new BufferStatusHub.BufferStatusNode(500));
    Assert.assertFalse(controller1.isUserCongested(context1));

    UserIdentifier user2 = new UserIdentifier("test2", "celeborn");
    UserCongestionControlContext context2 = controller1.getUserCongestionContext(user2);
    produceBytes(controller1, user2, 400);
    controller1.getProducedBufferStatusHub().add(new BufferStatusHub.BufferStatusNode(400));
    Assert.assertFalse(controller1.isUserCongested(context2));

    controller1.checkCongestion();
    Assert.assertTrue(controller1.isUserCongested(context1));
    Assert.assertFalse(controller1.isUserCongested(context2));

    Thread.sleep(1001);
    produceBytes(controller1, user1, 200);
    produceBytes(controller1, user1, 200);
    controller1.getProducedBufferStatusHub().add(new BufferStatusHub.BufferStatusNode(400));
    controller1.checkCongestion();
    Assert.assertFalse(controller1.isUserCongested(context1));
    Assert.assertFalse(controller1.isUserCongested(context2));
    controller1.close();
  }

  private void clearBufferStatus(CongestionController controller) {
    controller.getProducedBufferStatusHub().clear();
    controller.getConsumedBufferStatusHub().clear();
    for (UserCongestionControlContext context : controller.getUserCongestionContextMap().values()) {
      context.getUserBufferInfo().getBufferStatusHub().clear();
    }
  }
}
