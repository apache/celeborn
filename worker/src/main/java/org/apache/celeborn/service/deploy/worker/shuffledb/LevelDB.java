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

import org.iq80.leveldb.WriteOptions;

import org.apache.celeborn.common.metrics.source.AbstractSource;

/** Note: code copied from Apache Spark. */
public class LevelDB extends DB {
  private final org.iq80.leveldb.DB db;
  private final WriteOptions SYNC_WRITE_OPTIONS = new WriteOptions().sync(true);

  public LevelDB(org.iq80.leveldb.DB db, AbstractSource source, DBBackend dbBackend) {
    super(source, dbBackend);
    this.db = db;
  }

  @Override
  protected void putInternal(byte[] key, byte[] value) {
    db.put(key, value);
  }

  @Override
  protected void putInternal(byte[] key, byte[] value, boolean sync) {
    if (sync) {
      db.put(key, value, SYNC_WRITE_OPTIONS);
    } else {
      db.put(key, value);
    }
  }

  @Override
  protected byte[] getInternal(byte[] key) {
    return db.get(key);
  }

  @Override
  protected void deleteInternal(byte[] key) {
    db.delete(key);
  }

  @Override
  protected DBIterator newIterator(MetadataMetrics metrics) {
    return new LevelDBIterator(db.iterator(), metrics);
  }

  @Override
  public void close() throws IOException {
    db.close();
  }
}
