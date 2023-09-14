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

/** Note: code copied from Apache Spark. */
public class LevelDB implements DB {
  private final org.iq80.leveldb.DB db;
  private final WriteOptions SYNC_WRITE_OPTIONS = new WriteOptions().sync(true);

  public LevelDB(org.iq80.leveldb.DB db) {
    this.db = db;
  }

  @Override
  public void put(byte[] key, byte[] value) {
    db.put(key, value);
  }

  @Override
  public void put(byte[] key, byte[] value, boolean sync) {
    if (sync) {
      db.put(key, value, SYNC_WRITE_OPTIONS);
    } else {
      db.put(key, value);
    }
  }

  @Override
  public byte[] get(byte[] key) {
    return db.get(key);
  }

  @Override
  public void delete(byte[] key) {
    db.delete(key);
  }

  @Override
  public void close() throws IOException {
    db.close();
  }

  @Override
  public DBIterator iterator() {
    return new LevelDBIterator(db.iterator());
  }
}
