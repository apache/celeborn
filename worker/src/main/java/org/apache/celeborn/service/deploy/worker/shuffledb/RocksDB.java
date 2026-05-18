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

/**
 * RocksDB implementation of the local KV storage used to persist the shuffle state.
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
  }

  private void recreateDBInstance(long failedGeneration) {
    rwLock.writeLock().lock();
    try {
      if (dbGeneration.get() != failedGeneration) {
        logger.info(
            "RocksDB instance already recovered by another thread (generation {} -> {})",
            failedGeneration,
            dbGeneration.get());
        return;
      }

      try {
        if (db != null) {
          db.close();
        }
      } catch (Exception e) {
        logger.warn("Failed to close RocksDB instance", e);
      }
      db = RocksDBProvider.initRockDB(dbFile, version);
      dbGeneration.incrementAndGet();
    } catch (IOException e) {
      logger.error(
          "Failed to recreate RocksDB instance at {}. "
              + "Database is unavailable, all subsequent operations will fail.",
          dbFile,
          e);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  @Override
  protected void putInternal(byte[] key, byte[] value) throws RocksDBException {
    long generation = 0;
    try {
      rwLock.readLock().lock();
      generation = dbGeneration.get();
      try {
        db.put(key, value);
      } finally {
        rwLock.readLock().unlock();
      }
    } catch (RocksDBException e) {
      recreateDBInstance(generation);
      throw e;
    }
  }

  @Override
  protected void putInternal(byte[] key, byte[] value, boolean sync) throws RocksDBException {
    long generation = 0;
    try {
      rwLock.readLock().lock();
      generation = dbGeneration.get();
      try {
        if (sync) {
          db.put(SYNC_WRITE_OPTIONS, key, value);
        } else {
          db.put(key, value);
        }
      } finally {
        rwLock.readLock().unlock();
      }
    } catch (RocksDBException e) {
      recreateDBInstance(generation);
      throw e;
    }
  }

  @Override
  protected byte[] getInternal(byte[] key) throws RocksDBException {
    long generation = 0;
    try {
      rwLock.readLock().lock();
      generation = dbGeneration.get();
      try {
        return db.get(key);
      } finally {
        rwLock.readLock().unlock();
      }
    } catch (RocksDBException e) {
      recreateDBInstance(generation);
      throw e;
    }
  }

  @Override
  protected void deleteInternal(byte[] key) throws RocksDBException {
    long generation = 0;
    try {
      rwLock.readLock().lock();
      generation = dbGeneration.get();
      try {
        db.delete(key);
      } finally {
        rwLock.readLock().unlock();
      }
    } catch (RocksDBException e) {
      recreateDBInstance(generation);
      throw e;
    }
  }

  @Override
  protected DBIterator newIterator(MetadataMetrics metrics) {
    long generation = 0;
    try {
      rwLock.readLock().lock();
      generation = dbGeneration.get();
      try {
        return new RocksDBIterator(db.newIterator(), metrics);
      } finally {
        rwLock.readLock().unlock();
      }
    } catch (RuntimeException e) {
      recreateDBInstance(generation);
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    rwLock.writeLock().lock();
    try {
      db.close();
    } finally {
      rwLock.writeLock().unlock();
      SYNC_WRITE_OPTIONS.close();
    }
  }
}
