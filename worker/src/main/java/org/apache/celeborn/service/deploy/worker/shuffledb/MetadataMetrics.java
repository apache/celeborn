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

import scala.collection.immutable.Map;

import org.rocksdb.RocksDBException;

import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

/**
 * Records success/fail counters for metadata DB operations.
 *
 * <p>Callers express the operation kind (read or write) and the action; the recorder owns the
 * metric name, label selection, and the try/catch ceremony.
 */
class MetadataMetrics {

  @FunctionalInterface
  interface ThrowingRunnable {
    void run() throws RocksDBException;
  }

  @FunctionalInterface
  interface ThrowingSupplier<T> {
    T get() throws RocksDBException;
  }

  private final AbstractSource source;

  MetadataMetrics(AbstractSource source) {
    // A null source disables metrics but does not break DB operations: missing telemetry is
    // strictly preferable to failing every read/write. When non-null, idempotently register
    // the four label combinations so callers that have not pre-registered these counters do
    // not log a "Metric not found!" warning on every op (addCounter is putIfAbsent, so it is
    // safe to call even when WorkerSource has already registered them).
    this.source = source;
    if (source != null) {
      String name = WorkerSource.METADATA_OPERATION_STATUS_COUNT();
      source.addCounter(name, WorkerSource.WRITE_SUCCESS_COUNT_LABELS());
      source.addCounter(name, WorkerSource.WRITE_FAIL_COUNT_LABELS());
      source.addCounter(name, WorkerSource.READ_SUCCESS_COUNT_LABELS());
      source.addCounter(name, WorkerSource.READ_FAIL_COUNT_LABELS());
    }
  }

  void onWrite(ThrowingRunnable action) {
    record(
        () -> {
          action.run();
          return null;
        },
        WorkerSource.WRITE_SUCCESS_COUNT_LABELS(),
        WorkerSource.WRITE_FAIL_COUNT_LABELS());
  }

  void onRead(ThrowingRunnable action) {
    record(
        () -> {
          action.run();
          return null;
        },
        WorkerSource.READ_SUCCESS_COUNT_LABELS(),
        WorkerSource.READ_FAIL_COUNT_LABELS());
  }

  <T> T onRead(ThrowingSupplier<T> action) {
    return record(
        action, WorkerSource.READ_SUCCESS_COUNT_LABELS(), WorkerSource.READ_FAIL_COUNT_LABELS());
  }

  private <T> T record(
      ThrowingSupplier<T> action,
      Map<String, String> successLabels,
      Map<String, String> failLabels) {
    try {
      T result = action.get();
      incCounter(successLabels);
      return result;
    } catch (RocksDBException | RuntimeException e) {
      incCounter(failLabels);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  private void incCounter(Map<String, String> labels) {
    if (source != null) {
      source.incCounter(WorkerSource.METADATA_OPERATION_STATUS_COUNT(), 1, labels);
    }
  }
}
