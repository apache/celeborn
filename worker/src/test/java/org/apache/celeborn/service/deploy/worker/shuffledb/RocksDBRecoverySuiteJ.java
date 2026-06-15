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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

public class RocksDBRecoverySuiteJ {

  private File dbDir;
  private File dbFile;
  private CelebornConf defaultConf;
  private CelebornConf confWithRecovery;
  private WorkerSource workerSource;
  private StoreVersion version;

  @Before
  public void setUp() throws IOException {
    dbDir = Files.createTempDirectory("rocksdb-recovery-test").toFile();
    dbFile = new File(dbDir, "test-db");
    defaultConf = new CelebornConf();
    confWithRecovery = new CelebornConf();
    confWithRecovery.set(
        "celeborn.worker.graceful.shutdown.recoverDb.rocksdb.autoRecovery.enabled", "true");
    workerSource = new WorkerSource(defaultConf);
    version = new StoreVersion(1, 0);
  }

  @After
  public void tearDown() throws IOException {
    workerSource.destroy();
    JavaUtils.deleteRecursively(dbDir);
  }

  @Test
  public void testRecoveryAfterCorruption() throws Exception {
    DB db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSource, defaultConf);
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
    db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSource, defaultConf);
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
  public void testConcurrentRecoveryOnlyReopensOnce() throws Exception {
    DB db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSource, confWithRecovery);
    assertNotNull(db);

    byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    db.put(key, value);

    RocksDB rocksDB = (RocksDB) db;
    assertEquals(0, rocksDB.getDbGeneration());

    int threadCount = 8;
    CyclicBarrier barrier = new CyclicBarrier(threadCount);
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    // Trigger recovery directly to verify concurrent recovery deduplication
    rocksDB.forceRecovery();
    long genAfterFirstRecovery = rocksDB.getDbGeneration();
    assertEquals(1, genAfterFirstRecovery);

    // Now launch concurrent threads that all try to trigger recovery at the same generation.
    // genAfterFirstRecovery is captured once so every thread calls forceRecovery with the
    // same stale-generation value; only one can win the write-lock check and actually reopen.
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      futures.add(
          executor.submit(
              () -> {
                try {
                  barrier.await();
                  rocksDB.forceRecovery(genAfterFirstRecovery);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }));
    }

    try {
      for (Future<?> f : futures) {
        f.get();
      }
    } finally {
      ThreadUtils.shutdown(executor);
    }

    // Generation should have incremented exactly once more (all threads saw the same generation
    // and only one wins the write lock to perform the actual reopen; the rest observe the
    // advanced generation and bail without reopening)
    assertEquals(genAfterFirstRecovery + 1, rocksDB.getDbGeneration());

    // DB should still be usable
    db.put(key, value);
    byte[] result = db.get(key);
    assertNotNull(result);
    assertEquals("value", new String(result, StandardCharsets.UTF_8));

    db.close();
  }

  @Test
  public void testOperationsAfterCloseDoNotResurrect() throws Exception {
    DB db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSource, confWithRecovery);
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
    DB db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSource, confWithRecovery);
    assertNotNull(db);

    byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    db.put(key, value);

    // Get an iterator at generation 0
    DBIterator iter = db.iterator();
    iter.seek(key);
    assertTrue(iter.hasNext());

    // Force a recovery so the generation increments
    RocksDB rocksDB = (RocksDB) db;
    assertEquals(0, rocksDB.getDbGeneration());
    rocksDB.forceRecovery();
    assertEquals(1, rocksDB.getDbGeneration());

    // The stale iterator should throw on hasNext, next, and seek
    assertThrows(IllegalStateException.class, iter::hasNext);
    assertThrows(IllegalStateException.class, iter::next);
    assertThrows(IllegalStateException.class, () -> iter.seek(key));

    // A new iterator should work fine
    DBIterator newIter = db.iterator();
    newIter.seek(key);
    assertTrue(newIter.hasNext());
    newIter.close();

    db.close();
  }

  @Test
  public void testWithRecoveryTriggeredByRocksDBException() throws Exception {
    DB db = DBProvider.initDB(DBBackend.ROCKSDB, dbFile, version, workerSource, confWithRecovery);
    assertNotNull(db);

    byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    db.put(key, value);
    assertEquals("value", new String(db.get(key), StandardCharsets.UTF_8));

    RocksDB rocksDB = (RocksDB) db;
    assertEquals(0, rocksDB.getDbGeneration());

    // Swap the internal ManagedRocksDB with a Mockito spy that throws on the first put.
    // Recovery will replace this spy with a fresh instance, so subsequent puts succeed normally.
    Field dbField = RocksDB.class.getDeclaredField("db");
    dbField.setAccessible(true);
    ManagedRocksDB original = (ManagedRocksDB) dbField.get(rocksDB);
    ManagedRocksDB spied = spy(original);
    doThrow(new RocksDBException("injected failure"))
        .doCallRealMethod()
        .when(spied)
        .put(any(byte[].class), any(byte[].class));
    dbField.set(rocksDB, spied);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                db.put(
                    "k2".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8)));
    assertTrue(thrown.getCause() instanceof RocksDBException);

    // Recovery should have incremented the generation
    assertEquals(1, rocksDB.getDbGeneration());

    // Subsequent operations should succeed on the recovered DB
    byte[] newKey = "after-recovery".getBytes(StandardCharsets.UTF_8);
    byte[] newValue = "works".getBytes(StandardCharsets.UTF_8);
    db.put(newKey, newValue);
    assertEquals("works", new String(db.get(newKey), StandardCharsets.UTF_8));

    db.close();
  }

  private void corruptDbFiles(File dir) throws IOException {
    if (dir.isDirectory()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File f : files) {
          if (f.isFile()
              && (f.getName().endsWith(".sst")
                  || f.getName().startsWith("MANIFEST-")
                  || f.getName().equals("CURRENT"))) {
            Files.write(f.toPath(), "corrupted-data".getBytes(StandardCharsets.UTF_8));
          }
        }
      }
    }
  }
}
