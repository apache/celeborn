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

package org.apache.celeborn.client;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import scala.Option;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.StatusCode;

public class ClientThreadErrorLoggingSuiteJ {

  @Test
  public void testChangePartitionBackgroundErrorIsLogged() throws Exception {
    int shuffleId = 1;
    int partitionId = 2;
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.CLIENT_BATCH_HANDLE_CHANGE_PARTITION_INTERVAL().key(), "10ms");
    LifecycleManager lifecycleManager = mock(LifecycleManager.class);
    CommitManager commitManager = mock(CommitManager.class);
    when(lifecycleManager.commitManager()).thenReturn(commitManager);
    when(lifecycleManager.latestPartitionLocation()).thenReturn(new ConcurrentHashMap<>());

    AssertionError expected = new AssertionError("change partition failed");
    FailingChangePartitionManager manager =
        new FailingChangePartitionManager(conf, lifecycleManager, expected);
    manager.handleRequestPartitionLocation(
        new NoOpRequestLocationCallContext(),
        shuffleId,
        partitionId,
        0,
        newPartition(partitionId),
        Option.empty(),
        false);
    MessageAppender appender =
        new MessageAppender("Batch handle change partition for shuffle 1 failed.");

    Logger rootLogger = (Logger) LogManager.getRootLogger();
    rootLogger.addAppender(appender);
    try {
      manager.start();
      assertTrue("The change-partition error was not logged", appender.await());
      assertSame(expected, appender.loggingEvent.getThrown());
    } finally {
      manager.stop();
      shutdownExecutor(manager, "batchHandleChangePartitionExecutors");
      rootLogger.removeAppender(appender);
      appender.stop();
    }
  }

  @Test
  public void testReleasePartitionBackgroundErrorIsLogged() throws Exception {
    int shuffleId = 1;
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.CLIENT_BATCH_HANDLED_RELEASE_PARTITION_INTERVAL().key(), "10ms");
    LifecycleManager lifecycleManager = mock(LifecycleManager.class);
    AssertionError expected = new AssertionError("release partition failed");
    when(lifecycleManager.workerSnapshots(shuffleId)).thenThrow(expected);

    ReleasePartitionManager manager = new ReleasePartitionManager(conf, lifecycleManager);
    manager.releasePartition(shuffleId, 2);
    MessageAppender appender =
        new MessageAppender("Error releasing partition resource for shuffle 1");

    Logger rootLogger = (Logger) LogManager.getRootLogger();
    rootLogger.addAppender(appender);
    try {
      manager.start();
      assertTrue("The release-partition error was not logged", appender.await());
      assertSame(expected, appender.loggingEvent.getThrown());
    } finally {
      manager.stop();
      shutdownExecutor(manager, "batchHandleReleasePartitionExecutors");
      rootLogger.removeAppender(appender);
      appender.stop();
    }
  }

  private static PartitionLocation newPartition(int partitionId) {
    return new PartitionLocation(
        partitionId, 0, "localhost", 1, 2, 3, 4, PartitionLocation.Mode.PRIMARY);
  }

  private static void shutdownExecutor(Object target, String fieldSuffix) throws Exception {
    Class<?> currentClass = target.getClass();
    while (currentClass != null) {
      for (Field field : currentClass.getDeclaredFields()) {
        if (field.getName().endsWith(fieldSuffix)) {
          field.setAccessible(true);
          ((ExecutorService) field.get(target)).shutdownNow();
          return;
        }
      }
      currentClass = currentClass.getSuperclass();
    }
    throw new NoSuchFieldException(fieldSuffix);
  }

  private static class FailingChangePartitionManager extends ChangePartitionManager {
    private final Throwable failure;

    FailingChangePartitionManager(
        CelebornConf conf, LifecycleManager lifecycleManager, Throwable failure) {
      super(conf, lifecycleManager);
      this.failure = failure;
    }

    @Override
    public void handleRequestPartitions(
        int shuffleId,
        ChangePartitionRequest[] changePartitions,
        boolean isSegmentGranularityVisible) {
      ClientThreadErrorLoggingSuiteJ.<RuntimeException>throwUnchecked(failure);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
    throw (T) failure;
  }

  private static class NoOpRequestLocationCallContext implements RequestLocationCallContext {
    @Override
    public void reply(
        int partitionId,
        StatusCode status,
        Option<PartitionLocation> partitionLocation,
        boolean available) {}
  }

  private static class MessageAppender extends AbstractAppender {
    private final String expectedMessage;
    private final CountDownLatch logged = new CountDownLatch(1);
    private volatile LogEvent loggingEvent;

    MessageAppender(String expectedMessage) {
      super("MessageAppender", null, null, false);
      this.expectedMessage = expectedMessage;
      start();
    }

    @Override
    public void append(LogEvent event) {
      if (expectedMessage.equals(event.getMessage().getFormattedMessage())) {
        loggingEvent = event.toImmutable();
        logged.countDown();
      }
    }

    boolean await() throws InterruptedException {
      return logged.await(5, TimeUnit.SECONDS);
    }
  }
}
