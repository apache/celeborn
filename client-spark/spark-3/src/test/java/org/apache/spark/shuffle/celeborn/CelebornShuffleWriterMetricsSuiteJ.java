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

import static org.junit.Assert.assertEquals;

import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.junit.Test;

public class CelebornShuffleWriterMetricsSuiteJ {
  private static class BaseMetricsReporter implements ShuffleWriteMetricsReporter {
    long bytesWritten = 0L;

    @Override
    public void incBytesWritten(long v) {
      bytesWritten += v;
    }

    @Override
    public void incRecordsWritten(long v) {}

    @Override
    public void incWriteTime(long v) {}

    @Override
    public void decBytesWritten(long v) {}

    @Override
    public void decRecordsWritten(long v) {}
  }

  private static class CelebornMetricsReporter extends BaseMetricsReporter {
    long writerPayloadCount = 0L;
    long pushDataCount = 0L;
    long pushDataRetryCount = 0L;
    long pushDataTime = 0L;
    long inFlightWaitTime = 0L;

    public void incCelebornWriterPayloadCount(long v) {
      writerPayloadCount += v;
    }

    public void incCelebornPushDataCount(long v) {
      pushDataCount += v;
    }

    public void incCelebornPushDataRetryCount(long v) {
      pushDataRetryCount += v;
    }

    public void incCelebornPushDataTime(long v) {
      pushDataTime += v;
    }

    public void incCelebornInFlightWaitTime(long v) {
      inFlightWaitTime += v;
    }
  }

  private static class FailingCelebornMetricsReporter extends BaseMetricsReporter {
    int writerPayloadAttempts = 0;

    public void incCelebornWriterPayloadCount(long v) {
      writerPayloadAttempts += 1;
      throw new IllegalStateException("metric sink unavailable");
    }
  }

  @Test
  public void testForwardCelebornShuffleWriteMetricsWhenSparkExposesThem() {
    CelebornMetricsReporter metrics = new CelebornMetricsReporter();
    CelebornShuffleWriterMetrics celebornMetrics = new CelebornShuffleWriterMetrics(metrics);

    celebornMetrics.recordPushData(11L);
    celebornMetrics.recordPushData(12L);
    celebornMetrics.incPushDataCount(13L);
    celebornMetrics.incPushDataRetryCount(14L);
    celebornMetrics.incPushDataTime(15L);
    celebornMetrics.incInFlightWaitTime(16L);

    assertEquals(23L, metrics.bytesWritten);
    assertEquals(2L, metrics.writerPayloadCount);
    assertEquals(13L, metrics.pushDataCount);
    assertEquals(14L, metrics.pushDataRetryCount);
    assertEquals(15L, metrics.pushDataTime);
    assertEquals(16L, metrics.inFlightWaitTime);
  }

  @Test
  public void testIgnoreCelebornShuffleWriteMetricsWhenSparkDoesNotExposeThem() {
    BaseMetricsReporter metrics = new BaseMetricsReporter();
    CelebornShuffleWriterMetrics celebornMetrics = new CelebornShuffleWriterMetrics(metrics);

    celebornMetrics.recordPushData(11L);
    celebornMetrics.incPushDataCount(12L);
    celebornMetrics.incPushDataRetryCount(13L);
    celebornMetrics.incPushDataTime(14L);
    celebornMetrics.incInFlightWaitTime(15L);

    assertEquals(11L, metrics.bytesWritten);
  }

  @Test
  public void testDisableCelebornShuffleWriteMetricsAfterReflectiveInvocationFailure() {
    FailingCelebornMetricsReporter metrics = new FailingCelebornMetricsReporter();
    CelebornShuffleWriterMetrics celebornMetrics = new CelebornShuffleWriterMetrics(metrics);

    celebornMetrics.recordPushData(11L);
    celebornMetrics.recordPushData(12L);

    assertEquals(23L, metrics.bytesWritten);
    assertEquals(1, metrics.writerPayloadAttempts);
  }
}
