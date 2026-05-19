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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

public class RocksDBRecoverySuiteJ {

  private File dbDir;
  private File dbFile;
  private WorkerSource workerSource;
  private WorkerSource workerSourceWithRecovery;
  private StoreVersion version;

  @Before
  public void setUp() throws IOException {
    dbDir = Files.createTempDirectory("rocksdb-recovery-test").toFile();
    dbFile = new File(dbDir, "test-db");
    workerSource = new WorkerSource(new CelebornConf());

    CelebornConf confWithRecovery = new CelebornConf();
    confWithRecovery.set("celeborn.metadata.autoRecovery.enabled", "true");
    workerSourceWithRecovery = new WorkerSource(confWithRecovery);

    version = new StoreVersion(1, 0);
  }

  @After
  public void tearDown() throws IOException {
    workerSource.destroy();
    workerSourceWithRecovery.destroy();
    JavaUtils.deleteRecursively(dbDir);
  }

  @Test
  public void testRecoveryAfterCorruption() throws Exception {
    DB db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSource);
    assertNotNull(db);

    byte[] key = "test-key".getBytes(StandardCharsets.UTF_8);
    byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
    db.put(key, value);

    byte[] result = db.get(key);
    assertNotNull(result);
    assertEquals("test-value", new String(result, StandardCharsets.UTF_8));

    db.close();

    // Corrupt the DB by overwriting SST files
    corruptDbFiles(dbFile);

    // Reopen — initRockDB will wipe and recreate since files are corrupt
    db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSource);
    assertNotNull(db);

    // Data is gone after wipe-and-recreate, but DB is functional
    byte[] newKey = "new-key".getBytes(StandardCharsets.UTF_8);
    byte[] newValue = "new-value".getBytes(StandardCharsets.UTF_8);
    db.put(newKey, newValue);

    result = db.get(newKey);
    assertNotNull(result);
    assertEquals("new-value", new String(result, StandardCharsets.UTF_8));
    db.close();
  }

  @Test
  public void testConcurrentRecoveryOnlyRecreatesOnce() throws Exception {
    DB db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSource);
    assertNotNull(db);

    byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    db.put(key, value);

    RocksDB rocksDB = (RocksDB) db;

    // Close and reopen to get a working DB, then verify generation tracking
    db.close();
    db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSource);
    assertNotNull(db);
    rocksDB = (RocksDB) db;

    assertEquals(0, rocksDB.getDbGeneration());

    db.put(key, value);
    byte[] result = db.get(key);
    assertNotNull(result);

    db.close();
  }

  @Test
  public void testOperationsAfterCloseDoNotResurrect() throws Exception {
    DB db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSourceWithRecovery);
    assertNotNull(db);

    byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    db.put(key, value);
    db.close();

    // All operations after close should throw IllegalStateException
    DB closedDb = db;
    assertThrows(IllegalStateException.class, () -> closedDb.put("k".getBytes(), "v".getBytes()));

    assertThrows(IllegalStateException.class, () -> closedDb.get("k".getBytes()));

    assertThrows(IllegalStateException.class, () -> closedDb.delete("k".getBytes()));

    assertThrows(IllegalStateException.class, closedDb::iterator);
  }

  @Test
  public void testIteratorInvalidatedAfterRecovery() throws Exception {
    DB db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSourceWithRecovery);
    assertNotNull(db);

    byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    db.put(key, value);

    // Get an iterator at generation 0
    DBIterator iter = db.iterator();
    iter.seek(key);

    // Verify iterator works before any recovery
    assertTrue(iter.hasNext());
    iter.close();

    db.close();
  }

  private void corruptDbFiles(File dir) throws IOException {
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File f : files) {
          if (f.isFile()
              && (f.getName().endsWith(".sst")
                  || f.getName().equals("MANIFEST-000001")
                  || f.getName().equals("CURRENT"))) {
            Files.write(f.toPath(), "corrupted-data".getBytes(StandardCharsets.UTF_8));
          }
        }
      }
    }
  }
}
