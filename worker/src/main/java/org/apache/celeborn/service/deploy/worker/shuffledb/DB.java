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

import java.io.Closeable;

import org.apache.celeborn.common.metrics.source.AbstractSource;

/**
 * Common base class for the metadata KV store.
 *
 * <p>This class owns the {@link MetadataMetrics} recorder so that every backend (RocksDB, LevelDB,
 * ...) gets read/write success/fail counters for free, without having to repeat the metric plumbing
 * in each implementation. Subclasses only implement the raw backend-specific primitives ({@code
 * putInternal}, {@code getInternal}, {@code deleteInternal}, {@code newIterator}); the public API
 * methods below are {@code final} and wrap those primitives with the metric recording.
 *
 * <p>Note: code copied from Apache Spark.
 */
public abstract class DB implements Closeable {

  private final MetadataMetrics metrics;

  protected DB(AbstractSource source, DBBackend dbBackend) {
    this.metrics = new MetadataMetrics(source, dbBackend);
  }

  /** Set the DB entry for "key" to "value". */
  public final void put(byte[] key, byte[] value) {
    metrics.onWrite(() -> putInternal(key, value));
  }

  /** Set the DB entry for "key" to "value". Support Sync option. */
  public final void put(byte[] key, byte[] value, boolean sync) {
    metrics.onWrite(() -> putInternal(key, value, sync));
  }

  /**
   * Get which returns a new byte array storing the value associated with the specified input key if
   * any.
   */
  public final byte[] get(byte[] key) {
    return metrics.onRead(() -> getInternal(key));
  }

  /** Delete the DB entry (if any) for "key". */
  public final void delete(byte[] key) {
    metrics.onWrite(() -> deleteInternal(key));
  }

  /** Return an iterator over the contents of the DB. */
  public final DBIterator iterator() {
    return newIterator(metrics);
  }

  protected abstract void putInternal(byte[] key, byte[] value) throws Exception;

  protected abstract void putInternal(byte[] key, byte[] value, boolean sync) throws Exception;

  protected abstract byte[] getInternal(byte[] key) throws Exception;

  protected abstract void deleteInternal(byte[] key) throws Exception;

  protected abstract DBIterator newIterator(MetadataMetrics metrics);
}
