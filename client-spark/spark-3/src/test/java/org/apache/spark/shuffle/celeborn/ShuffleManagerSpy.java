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

import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.shuffle.ShuffleReader;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;

public class ShuffleManagerSpy extends SparkShuffleManager {

  private static final AtomicReference<OpenShuffleReaderCallback> getShuffleReaderHook =
      new AtomicReference<>();
  private static final AtomicReference<CelebornConf> configurationHolder = new AtomicReference<>();

  public ShuffleManagerSpy(SparkConf conf, boolean isDriver) {
    super(conf, isDriver);
  }

  @Override
  public <K, C> ShuffleReader<K, C> getCelebornShuffleReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      int startMapIndex,
      int endMapIndex,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    OpenShuffleReaderCallback consumer = getShuffleReaderHook.get();
    if (consumer != null) {
      CelebornShuffleHandle celebornShuffleHandle = (CelebornShuffleHandle) handle;
      ShuffleClient client =
          ShuffleClient.get(
              celebornShuffleHandle.appUniqueId(),
              celebornShuffleHandle.lifecycleManagerHost(),
              celebornShuffleHandle.lifecycleManagerPort(),
              configurationHolder.get(),
              celebornShuffleHandle.userIdentifier());
      consumer.accept(
          celebornShuffleHandle.appUniqueId(),
          celebornShuffleHandle.shuffleId(),
          client,
          startPartition,
          endPartition);
    }
    return super.getCelebornShuffleReader(
        handle, startPartition, endPartition, startMapIndex, endMapIndex, context, metrics);
  }

  public interface OpenShuffleReaderCallback {
    void accept(
        String appId,
        Integer shuffleId,
        ShuffleClient client,
        Integer startPartition,
        Integer endPartition);
  }

  public static void interceptOpenShuffleReader(OpenShuffleReaderCallback hook, CelebornConf conf) {
    getShuffleReaderHook.set(hook);
    configurationHolder.set(conf);
  }

  public static void resetHook() {
    getShuffleReaderHook.set(null);
    configurationHolder.set(null);
  }
}
