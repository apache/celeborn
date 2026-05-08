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

import java.lang.reflect.Method;
import java.util.function.LongConsumer;

import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;

import org.apache.celeborn.common.write.PushMetricsCallback;

final class CelebornShuffleWriterMetrics implements PushMetricsCallback {

  private final ShuffleWriteMetricsReporter metrics;
  private final LongConsumer incCelebornWriterPayloadCount;
  private final LongConsumer incCelebornPushDataCount;
  private final LongConsumer incCelebornPushDataRetryCount;
  private final LongConsumer incCelebornPushDataTime;
  private final LongConsumer incCelebornInFlightWaitTime;

  CelebornShuffleWriterMetrics(ShuffleWriteMetricsReporter metrics) {
    this.metrics = metrics;
    this.incCelebornWriterPayloadCount =
        optionalMetricUpdate(metrics, "incCelebornWriterPayloadCount");
    this.incCelebornPushDataCount =
        optionalMetricUpdate(metrics, "incCelebornPushDataCount");
    this.incCelebornPushDataRetryCount =
        optionalMetricUpdate(metrics, "incCelebornPushDataRetryCount");
    this.incCelebornPushDataTime =
        optionalMetricUpdate(metrics, "incCelebornPushDataTime");
    this.incCelebornInFlightWaitTime =
        optionalMetricUpdate(metrics, "incCelebornInFlightWaitTime");
  }

  void recordPushData(long bytesWritten) {
    metrics.incBytesWritten(bytesWritten);
    incCelebornWriterPayloadCount.accept(1L);
  }

  @Override
  public void incPushDataCount(long count) {
    incCelebornPushDataCount.accept(count);
  }

  @Override
  public void incPushDataRetryCount(long count) {
    incCelebornPushDataRetryCount.accept(count);
  }

  @Override
  public void incPushDataTime(long time) {
    incCelebornPushDataTime.accept(time);
  }

  @Override
  public void incInFlightWaitTime(long time) {
    incCelebornInFlightWaitTime.accept(time);
  }

  private static LongConsumer optionalMetricUpdate(
      ShuffleWriteMetricsReporter metrics, String methodName) {
    try {
      Method method = metrics.getClass().getMethod(methodName, long.class);
      method.setAccessible(true);
      return new OptionalMetricUpdate(metrics, method);
    } catch (ReflectiveOperationException | RuntimeException e) {
      return CelebornShuffleWriterMetrics::noopMetricUpdate;
    }
  }

  private static void noopMetricUpdate(long value) {}

  private static final class OptionalMetricUpdate implements LongConsumer {
    private final ShuffleWriteMetricsReporter metrics;
    private final Method method;
    private volatile boolean enabled = true;

    private OptionalMetricUpdate(ShuffleWriteMetricsReporter metrics, Method method) {
      this.metrics = metrics;
      this.method = method;
    }

    @Override
    public void accept(long value) {
      if (enabled) {
        try {
          method.invoke(metrics, value);
        } catch (ReflectiveOperationException | RuntimeException e) {
          enabled = false;
        }
      }
    }
  }
}
