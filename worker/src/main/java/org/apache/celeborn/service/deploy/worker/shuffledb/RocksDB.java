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

import java.io.IOException;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import org.apache.celeborn.common.metrics.source.AbstractSource;

/**
 * RocksDB implementation of the local KV storage used to persist the shuffle state.
 *
 * <p>Note: code copied from Apache Spark.
 */
public class RocksDB extends DB {
  private final org.rocksdb.RocksDB db;
  private final WriteOptions SYNC_WRITE_OPTIONS = new WriteOptions().setSync(true);

  public RocksDB(org.rocksdb.RocksDB db, AbstractSource source, DBBackend dbBackend) {
    super(source, dbBackend);
    this.db = db;
  }

  @Override
  protected void putInternal(byte[] key, byte[] value) throws RocksDBException {
    db.put(key, value);
  }

  @Override
  protected void putInternal(byte[] key, byte[] value, boolean sync) throws RocksDBException {
    if (sync) {
      db.put(SYNC_WRITE_OPTIONS, key, value);
    } else {
      db.put(key, value);
    }
  }

  @Override
  protected byte[] getInternal(byte[] key) throws RocksDBException {
    return db.get(key);
  }

  @Override
  protected void deleteInternal(byte[] key) throws RocksDBException {
    db.delete(key);
  }

  @Override
  protected DBIterator newIterator(MetadataMetrics metrics) {
    return new RocksDBIterator(db.newIterator(), metrics);
  }

  @Override
  public void close() throws IOException {
    try {
      db.close();
    } finally {
      // WriteOptions is a native handle; release it even if db.close() throws.
      SYNC_WRITE_OPTIONS.close();
    }
  }
}
