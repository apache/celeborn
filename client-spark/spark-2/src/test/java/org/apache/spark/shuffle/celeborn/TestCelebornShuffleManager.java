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

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.*;

public class TestCelebornShuffleManager extends SparkShuffleManager {

  private static ShuffleManagerHook shuffleReaderGetHook = null;
  private static ShuffleManagerHook shuffleWriterGetHook = null;

  public TestCelebornShuffleManager(SparkConf conf) {
    super(conf, true);
  }

  public static void registerReaderGetHook(ShuffleManagerHook hook) {
    shuffleReaderGetHook = hook;
  }

  public static void registerWriterGetHook(ShuffleManagerHook hook) {
    shuffleWriterGetHook = hook;
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle, int startPartition, int endPartition, TaskContext context) {
    if (shuffleReaderGetHook != null) {
      shuffleReaderGetHook.exec(handle, startPartition, endPartition, context);
    }
    return super.getReader(handle, startPartition, endPartition, context);
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    if (shuffleWriterGetHook != null) {
      shuffleWriterGetHook.exec(handle, mapId, context);
    }
    return super.getWriter(handle, mapId, context, metrics);
  }
}
