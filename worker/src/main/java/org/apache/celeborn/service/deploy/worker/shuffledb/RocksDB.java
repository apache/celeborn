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

import org.rocksdb.WriteOptions;

import org.apache.celeborn.common.metrics.source.AbstractSource;

/**
 * RocksDB implementation of the local KV storage used to persist the shuffle state.
 *
 * <p>Note: code copied from Apache Spark.
 */
public class RocksDB implements DB {
  private final org.rocksdb.RocksDB db;
  private final WriteOptions SYNC_WRITE_OPTIONS = new WriteOptions().setSync(true);
  private final MetadataMetrics metrics;

  public RocksDB(org.rocksdb.RocksDB db, AbstractSource source) {
    this.db = db;
    this.metrics = new MetadataMetrics(source);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    metrics.onWrite(() -> db.put(key, value));
  }

  @Override
  public void put(byte[] key, byte[] value, boolean sync) {
    metrics.onWrite(
        () -> {
          if (sync) {
            db.put(SYNC_WRITE_OPTIONS, key, value);
          } else {
            db.put(key, value);
          }
        });
  }

  @Override
  public byte[] get(byte[] key) {
    return metrics.onRead(() -> db.get(key));
  }

  @Override
  public void delete(byte[] key) {
    metrics.onWrite(() -> db.delete(key));
  }

  @Override
  public DBIterator iterator() {
    return new RocksDBIterator(db.newIterator(), metrics);
  }

  @Override
  public void close() throws IOException {
    db.close();
  }
}
