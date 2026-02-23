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

public class ShuffleManagerSpy extends SparkShuffleManager {

  private static AtomicReference<Callback<?, ?, ?>> getShuffleReaderHook = new AtomicReference<>();

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
    Callback consumer = getShuffleReaderHook.get();
    if (consumer != null) {
      CelebornShuffleHandle celebornShuffleHandle = (CelebornShuffleHandle) handle;
      consumer.accept(celebornShuffleHandle, startPartition, endPartition);
    }
    return super.getCelebornShuffleReader(
        handle, startPartition, endPartition, startMapIndex, endMapIndex, context, metrics);
  }

  public interface Callback<K, V, T> {
    void accept(
        CelebornShuffleHandle<K, V, T> handle, Integer startPartition, Integer endPartition);
  }

  public static <K, V, T> void interceptOpenShuffleReader(Callback<K, V, T> hook) {
    getShuffleReaderHook.set(hook);
  }

  public static void resetHook() {
    getShuffleReaderHook.set(null);
  }
}
