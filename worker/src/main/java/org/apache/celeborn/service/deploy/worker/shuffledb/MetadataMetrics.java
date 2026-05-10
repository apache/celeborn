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
  private final Map<String, String> writeSuccessLabels;
  private final Map<String, String> writeFailLabels;
  private final Map<String, String> readSuccessLabels;
  private final Map<String, String> readFailLabels;

  MetadataMetrics(AbstractSource source, DBBackend dbBackend) {
    // Register the four label combinations so callers see the counters even before the first op
    // (addCounter is putIfAbsent, so the call is safe even if something else registered them).
    this.source = source;
    String backend = dbBackend.name();

    this.writeSuccessLabels = WorkerSource.WRITE_SUCCESS_COUNT_LABELS(backend);
    this.writeFailLabels = WorkerSource.WRITE_FAIL_COUNT_LABELS(backend);
    this.readSuccessLabels = WorkerSource.READ_SUCCESS_COUNT_LABELS(backend);
    this.readFailLabels = WorkerSource.READ_FAIL_COUNT_LABELS(backend);
    if (source != null) {
      String name = WorkerSource.METADATA_OPERATION_STATUS_COUNT();
      source.addCounter(name, writeSuccessLabels);
      source.addCounter(name, writeFailLabels);
      source.addCounter(name, readSuccessLabels);
      source.addCounter(name, readFailLabels);
    }
  }

  void onWrite(ThrowingRunnable action) {
    record(
        () -> {
          action.run();
          return null;
        },
        writeSuccessLabels,
        writeFailLabels);
  }

  void onRead(ThrowingRunnable action) {
    record(
        () -> {
          action.run();
          return null;
        },
        readSuccessLabels,
        readFailLabels);
  }

  <T> T onRead(ThrowingSupplier<T> action) {
    return record(action, readSuccessLabels, readFailLabels);
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
