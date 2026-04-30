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

package org.apache.celeborn.service.deploy.worker.shuffledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicInteger;

import scala.collection.JavaConverters;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.rocksdb.RocksDBException;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.metrics.source.NamedCounter;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

public class MetadataMetricsSuiteJ {

  private static final String METRIC = WorkerSource.METADATA_OPERATION_STATUS_COUNT();

  private WorkerSource mockSource;

  @Before
  public void setUp() {
    mockSource = Mockito.mock(WorkerSource.class);
  }

  @Test
  public void nullSourceConstructorDoesNotThrow() {
    new MetadataMetrics(null);
  }

  @Test
  public void nullSourceOnWriteStillExecutesAction() {
    MetadataMetrics metrics = new MetadataMetrics(null);
    AtomicInteger invocations = new AtomicInteger();

    metrics.onWrite(invocations::incrementAndGet);

    assertEquals(1, invocations.get());
  }

  @Test
  public void nullSourceOnReadSupplierStillReturnsValue() {
    MetadataMetrics metrics = new MetadataMetrics(null);

    byte[] result = metrics.onRead((MetadataMetrics.ThrowingSupplier<byte[]>) () -> new byte[] {7});

    assertEquals(7, result[0]);
  }

  @Test
  public void nullSourceFailureStillRethrows() {
    MetadataMetrics metrics = new MetadataMetrics(null);
    RocksDBException cause = new RocksDBException("test");

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                metrics.onWrite(
                    () -> {
                      throw cause;
                    }));

    assertSame(cause, thrown.getCause());
  }

  @Test
  public void constructorRegistersAllFourLabelCombinations() {
    new MetadataMetrics(mockSource);

    verify(mockSource).addCounter(METRIC, WorkerSource.WRITE_SUCCESS_COUNT_LABELS());
    verify(mockSource).addCounter(METRIC, WorkerSource.WRITE_FAIL_COUNT_LABELS());
    verify(mockSource).addCounter(METRIC, WorkerSource.READ_SUCCESS_COUNT_LABELS());
    verify(mockSource).addCounter(METRIC, WorkerSource.READ_FAIL_COUNT_LABELS());
  }

  @Test
  public void onWriteRunnableSuccessIncrementsWriteSuccessCounter() {
    MetadataMetrics metrics = new MetadataMetrics(mockSource);
    AtomicInteger invocations = new AtomicInteger();

    metrics.onWrite(invocations::incrementAndGet);

    assertEquals(1, invocations.get());
    verify(mockSource).incCounter(METRIC, 1L, WorkerSource.WRITE_SUCCESS_COUNT_LABELS());
    verify(mockSource, never()).incCounter(METRIC, 1L, WorkerSource.WRITE_FAIL_COUNT_LABELS());
  }

  @Test
  public void onWriteRunnableRocksDBExceptionIsWrappedAndFailCounted() {
    MetadataMetrics metrics = new MetadataMetrics(mockSource);
    RocksDBException cause = new RocksDBException("test");

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                metrics.onWrite(
                    () -> {
                      throw cause;
                    }));

    assertSame(cause, thrown.getCause());
    verify(mockSource).incCounter(METRIC, 1L, WorkerSource.WRITE_FAIL_COUNT_LABELS());
    verify(mockSource, never()).incCounter(METRIC, 1L, WorkerSource.WRITE_SUCCESS_COUNT_LABELS());
  }

  @Test
  public void onWriteRunnableRuntimeExceptionPropagatesUnwrapped() {
    MetadataMetrics metrics = new MetadataMetrics(mockSource);
    IllegalStateException original = new IllegalStateException("test");

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                metrics.onWrite(
                    () -> {
                      throw original;
                    }));

    assertSame(original, thrown);
    verify(mockSource).incCounter(METRIC, 1L, WorkerSource.WRITE_FAIL_COUNT_LABELS());
  }

  @Test
  public void onReadRunnableSuccessIncrementsReadSuccessCounter() {
    MetadataMetrics metrics = new MetadataMetrics(mockSource);

    metrics.onRead((MetadataMetrics.ThrowingRunnable) () -> {});

    verify(mockSource).incCounter(METRIC, 1L, WorkerSource.READ_SUCCESS_COUNT_LABELS());
  }

  @Test
  public void onReadRunnableFailureIncrementsReadFailCounter() {
    MetadataMetrics metrics = new MetadataMetrics(mockSource);
    RocksDBException cause = new RocksDBException("test");

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                metrics.onRead(
                    (MetadataMetrics.ThrowingRunnable)
                        () -> {
                          throw cause;
                        }));

    assertSame(cause, thrown.getCause());
    verify(mockSource).incCounter(METRIC, 1L, WorkerSource.READ_FAIL_COUNT_LABELS());
  }

  @Test
  public void onReadSupplierReturnsValueOnSuccess() {
    MetadataMetrics metrics = new MetadataMetrics(mockSource);
    byte[] expected = new byte[] {1, 2, 3};

    byte[] result = metrics.onRead((MetadataMetrics.ThrowingSupplier<byte[]>) () -> expected);

    assertSame(expected, result);
    verify(mockSource).incCounter(METRIC, 1L, WorkerSource.READ_SUCCESS_COUNT_LABELS());
  }

  @Test
  public void onReadSupplierFailureIncrementsReadFailCounter() {
    MetadataMetrics metrics = new MetadataMetrics(mockSource);
    RocksDBException cause = new RocksDBException("test");

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                metrics.onRead(
                    (MetadataMetrics.ThrowingSupplier<Object>)
                        () -> {
                          throw cause;
                        }));

    assertSame(cause, thrown.getCause());
    verify(mockSource).incCounter(METRIC, 1L, WorkerSource.READ_FAIL_COUNT_LABELS());
  }

  @Test
  public void multipleOpsAccumulateCounterIncrements() {
    MetadataMetrics metrics = new MetadataMetrics(mockSource);

    metrics.onWrite(() -> {});
    metrics.onWrite(() -> {});
    metrics.onRead((MetadataMetrics.ThrowingRunnable) () -> {});

    verify(mockSource, times(2)).incCounter(METRIC, 1L, WorkerSource.WRITE_SUCCESS_COUNT_LABELS());
    verify(mockSource, times(1)).incCounter(METRIC, 1L, WorkerSource.READ_SUCCESS_COUNT_LABELS());
  }

  /**
   * End-to-end check using a real WorkerSource. This exercises the actual counter registration +
   * increment path without any mocks, and confirms the value-level effect on the counter.
   */
  @Test
  public void endToEndRealWorkerSourceTracksCounts() {
    WorkerSource source = new WorkerSource(new CelebornConf());
    try {
      MetadataMetrics metrics = new MetadataMetrics(source);

      metrics.onWrite(() -> {});
      metrics.onWrite(() -> {});
      metrics.onRead((MetadataMetrics.ThrowingRunnable) () -> {});
      assertThrows(
          RuntimeException.class,
          () ->
              metrics.onWrite(
                  () -> {
                    throw new RocksDBException("fail");
                  }));

      assertEquals(2L, counterCount(source, WorkerSource.WRITE_SUCCESS_COUNT_LABELS()));
      assertEquals(1L, counterCount(source, WorkerSource.WRITE_FAIL_COUNT_LABELS()));
      assertEquals(1L, counterCount(source, WorkerSource.READ_SUCCESS_COUNT_LABELS()));
      assertEquals(0L, counterCount(source, WorkerSource.READ_FAIL_COUNT_LABELS()));
    } finally {
      source.destroy();
    }
  }

  private static long counterCount(
      WorkerSource source, scala.collection.immutable.Map<String, String> labels) {
    String name = WorkerSource.METADATA_OPERATION_STATUS_COUNT();
    java.util.Map<String, String> wanted = JavaConverters.mapAsJavaMapConverter(labels).asJava();
    for (NamedCounter c : JavaConverters.seqAsJavaListConverter(source.counters()).asJava()) {
      if (!c.name().equals(name)) {
        continue;
      }
      java.util.Map<String, String> actual =
          JavaConverters.mapAsJavaMapConverter(c.labels()).asJava();
      if (containsAll(actual, wanted)) {
        return c.counter().getCount();
      }
    }
    fail("counter not found: " + name + " " + wanted);
    return -1L;
  }

  private static boolean containsAll(
      java.util.Map<String, String> actual, java.util.Map<String, String> required) {
    for (java.util.Map.Entry<String, String> e : required.entrySet()) {
      if (!e.getValue().equals(actual.get(e.getKey()))) {
        return false;
      }
    }
    return true;
  }
}
