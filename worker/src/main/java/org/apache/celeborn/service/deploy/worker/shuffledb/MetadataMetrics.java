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

import org.rocksdb.RocksDBException;

import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

import scala.collection.immutable.Map;

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
    this.source = source;
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
      ThrowingSupplier<T> action, Map<String, String> successLabels, Map<String, String> failLabels) {
    try {
      T result = action.get();
      source.incCounter(WorkerSource.METADATA_OPERATION_STATUS_COUNT(), 1, successLabels);
      return result;
    } catch (RocksDBException | RuntimeException e) {
      source.incCounter(WorkerSource.METADATA_OPERATION_STATUS_COUNT(), 1, failLabels);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }
}
