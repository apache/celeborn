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

import org.rocksdb.BloomFilter;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates a {@link org.rocksdb.RocksDB} instance together with the resources it depends on.
 * {@link org.rocksdb.RocksDB#close()} releases only the DB handle (and column family handles); the
 * {@link Options}, {@link BloomFilter}, and {@link org.rocksdb.Logger} each own separate off-heap
 * memory and must be closed explicitly.
 *
 * <p>Instances are produced by {@link RocksDBProvider}; callers interact with the underlying DB
 * through the forwarding {@code put/get/delete/newIterator} methods and release everything via a
 * single {@link #close()} call.
 */
public class ManagedRocksDB implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(ManagedRocksDB.class);

  private final Options dbOptions;
  private final BloomFilter bloomFilter;
  private final org.rocksdb.Logger rocksDBLogger;
  private org.rocksdb.RocksDB db;

  ManagedRocksDB(Options dbOptions, BloomFilter bloomFilter, org.rocksdb.Logger rocksDBLogger) {
    this.dbOptions = dbOptions;
    this.bloomFilter = bloomFilter;
    this.rocksDBLogger = rocksDBLogger;
  }

  /** Attaches the DB handle */
  void setDb(org.rocksdb.RocksDB db) {
    this.db = db;
  }

  /** Package-private accessor */
  Options options() {
    return dbOptions;
  }

  /** Package-private accessor for {@link RocksDBProvider#checkVersion}. */
  org.rocksdb.RocksDB db() {
    return db;
  }

  public void put(byte[] key, byte[] value) throws RocksDBException {
    db.put(key, value);
  }

  public void put(WriteOptions writeOptions, byte[] key, byte[] value) throws RocksDBException {
    db.put(writeOptions, key, value);
  }

  public byte[] get(byte[] key) throws RocksDBException {
    return db.get(key);
  }

  public void delete(byte[] key) throws RocksDBException {
    db.delete(key);
  }

  public RocksIterator newIterator() {
    return db.newIterator();
  }

  @Override
  public void close() {
    closeQuietly(db, "RocksDB");
    closeQuietly(dbOptions, "RocksDB Options");
    closeQuietly(bloomFilter, "RocksDB BloomFilter");
    closeQuietly(rocksDBLogger, "RocksDB logger");
  }

  private static void closeQuietly(AutoCloseable resource, String name) {
    if (resource == null) {
      return;
    }
    try {
      resource.close();
    } catch (Exception e) {
      logger.warn("Failed to close {}", name, e);
    }
  }
}
