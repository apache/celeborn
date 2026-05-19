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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

/**
 * RocksDB implementation of the local KV storage used to persist the shuffle state.
 *
 * <p>This class supports automatic recovery from RocksDB failures when {@code autoRecoveryEnabled}
 * is set to {@code true}. When a put/get/delete operation encounters a {@link RocksDBException},
 * the DB instance is closed and reopened. Recovery first attempts a safe reopen; if that fails, it
 * falls back to recreating the DB. When {@code autoRecoveryEnabled} is {@code false} (the default),
 * exceptions are propagated directly without any recovery attempt.
 *
 * <p>Iterators obtained via {@link #iterator()} are invalidated after a recovery event and will
 * throw {@link IllegalStateException} on subsequent use.
 *
 * <p>Note: code copied from Apache Spark.
 */
public class RocksDB extends DB {
  private static final Logger logger = LoggerFactory.getLogger(RocksDB.class);

  private volatile org.rocksdb.RocksDB db;
  private final WriteOptions SYNC_WRITE_OPTIONS = new WriteOptions().setSync(true);
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final AtomicLong dbGeneration = new AtomicLong(0);
  private final File dbFile;
  private final StoreVersion version;
  private final boolean autoRecoveryEnabled;
  private volatile boolean closed = false;

  public RocksDB(
      org.rocksdb.RocksDB db,
      AbstractSource source,
      DBBackend dbBackend,
      File dbFile,
      StoreVersion version) {
    super(source, dbBackend);
    this.db = db;
    this.dbFile = dbFile;
    this.version = version;
    this.autoRecoveryEnabled =
        (source instanceof WorkerSource) && ((WorkerSource) source).metadataAutoRecoveryEnabled();
  }

  private void recreateDBInstance(long failedGeneration) {
    if (isClosed()) {
      return;
    }

    rwLock.writeLock().lock();
    try {
      if (dbGeneration.get() != failedGeneration) {
        logger.info(
            "RocksDB instance already recovered by another thread (generation {} -> {})",
            failedGeneration,
            dbGeneration.get());
        return;
      }

      if (isClosed()) {
        return;
      }

      try {
        if (db != null) {
          db.close();
        }
      } catch (Exception e) {
        logger.warn("Failed to close RocksDB instance", e);
      }

      // Phase 1: try safe reopen
      try {
        db = RocksDBProvider.reopenRocksDB(dbFile);
        dbGeneration.incrementAndGet();
        logger.info("RocksDB instance recovered {}", dbFile);
        return;
      } catch (IOException e) {
        logger.warn("Safe reopen failed for RocksDB at {}", dbFile, e);
      }

      // Phase 2: recreate
      try {
        db = RocksDBProvider.initRockDB(dbFile, version);
        dbGeneration.incrementAndGet();
        logger.error("RocksDB {} was recreated.", dbFile);
      } catch (IOException e) {
        dbGeneration.incrementAndGet();
        logger.error("Failed to recreate RocksDB instance at {}. ", dbFile, e);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private void checkState() {
    if (isClosed()) {
      throw new IllegalStateException("DB is closed");
    }
  }

  private boolean isClosed() {
    return closed;
  }

  @FunctionalInterface
  interface CheckedSupplier<T> {
    T get() throws RocksDBException;
  }

  @FunctionalInterface
  interface CheckedRunnable {
    void run() throws RocksDBException;
  }

  private <T> T withRecovery(CheckedSupplier<T> operation) throws RocksDBException {
    checkState();
    rwLock.readLock().lock();
    boolean unlocked = false;
    long generation = dbGeneration.get();
    try {
      return operation.get();
    } catch (RocksDBException e) {
      rwLock.readLock().unlock();
      unlocked = true;
      if (autoRecoveryEnabled) {
        recreateDBInstance(generation);
      }
      throw e;
    } finally {
      if (!unlocked) {
        rwLock.readLock().unlock();
      }
    }
  }

  private void runWithRecovery(CheckedRunnable operation) throws RocksDBException {
    withRecovery(
        () -> {
          operation.run();
          return null;
        });
  }

  @Override
  protected void putInternal(byte[] key, byte[] value) throws RocksDBException {
    runWithRecovery(() -> db.put(key, value));
  }

  @Override
  protected void putInternal(byte[] key, byte[] value, boolean sync) throws RocksDBException {
    runWithRecovery(
        () -> {
          if (sync) {
            db.put(SYNC_WRITE_OPTIONS, key, value);
          } else {
            db.put(key, value);
          }
        });
  }

  @Override
  protected byte[] getInternal(byte[] key) throws RocksDBException {
    return withRecovery(() -> db.get(key));
  }

  @Override
  protected void deleteInternal(byte[] key) throws RocksDBException {
    runWithRecovery(() -> db.delete(key));
  }

  @Override
  protected DBIterator newIterator(MetadataMetrics metrics) {
    checkState();
    rwLock.readLock().lock();
    long generation = dbGeneration.get();
    try {
      return new RocksDBIterator(db.newIterator(), metrics, dbGeneration, generation);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {
    rwLock.writeLock().lock();
    try {
      closed = true;
      db.close();
    } finally {
      rwLock.writeLock().unlock();
      SYNC_WRITE_OPTIONS.close();
    }
  }

  // Visible for testing
  long getDbGeneration() {
    return dbGeneration.get();
  }
}
