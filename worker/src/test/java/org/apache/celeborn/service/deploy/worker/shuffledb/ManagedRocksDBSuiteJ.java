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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksIterator;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.util.JavaUtils;

public class ManagedRocksDBSuiteJ {

  static {
    // Ensure the native library is loaded for the partial-init test below, which constructs
    // RocksDB native resources directly without going through RocksDBProvider.
    org.rocksdb.RocksDB.loadLibrary();
  }

  private File dbDir;
  private File dbFile;
  private StoreVersion version;
  private CelebornConf conf;

  @Before
  public void setUp() throws IOException {
    dbDir = Files.createTempDirectory("managed-rocksdb-test").toFile();
    dbFile = new File(dbDir, "test-db");
    version = new StoreVersion(1, 0);
    conf = new CelebornConf();
  }

  @After
  public void tearDown() throws IOException {
    JavaUtils.deleteRecursively(dbDir);
  }

  @Test
  public void testForwardingPutGetDeleteDelegateToUnderlyingDb() throws Exception {
    ManagedRocksDB managedDb = RocksDBProvider.initRockDB(dbFile, version, conf);
    try {
      byte[] key = "key".getBytes(StandardCharsets.UTF_8);
      byte[] value = "value".getBytes(StandardCharsets.UTF_8);

      managedDb.put(key, value);
      assertArrayEquals(value, managedDb.get(key));

      managedDb.delete(key);
      assertNull(managedDb.get(key));
    } finally {
      managedDb.close();
    }
  }

  @Test
  public void testNewIteratorReturnsUsableIterator() throws Exception {
    ManagedRocksDB managedDb = RocksDBProvider.initRockDB(dbFile, version, conf);
    try {
      managedDb.put("a".getBytes(StandardCharsets.UTF_8), "1".getBytes(StandardCharsets.UTF_8));
      managedDb.put("b".getBytes(StandardCharsets.UTF_8), "2".getBytes(StandardCharsets.UTF_8));

      try (RocksIterator iter = managedDb.newIterator()) {
        int seen = 0;
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          seen++;
        }
        assertTrue("expected at least 3 entries from forwarded iterator, got " + seen, seen >= 3);
      }
    } finally {
      managedDb.close();
    }
  }

  @Test
  public void testCloseReleasesUnderlyingNativeHandles() throws Exception {
    ManagedRocksDB managedDb = RocksDBProvider.initRockDB(dbFile, version, conf);
    org.rocksdb.RocksDB underlyingDb = managedDb.db();
    Options underlyingOptions = managedDb.options();
    assertNotNull(underlyingDb);
    assertNotNull(underlyingOptions);
    assertTrue("underlying DB should own native handle pre-close", underlyingDb.isOwningHandle());
    assertTrue(
        "underlying Options should own native handle pre-close",
        underlyingOptions.isOwningHandle());

    managedDb.close();

    assertFalse(
        "underlying DB native handle must be released after close", underlyingDb.isOwningHandle());
    assertFalse(
        "underlying Options native handle must be released after close",
        underlyingOptions.isOwningHandle());
  }

  @Test
  public void testCloseIsIdempotent() throws Exception {
    ManagedRocksDB managedDb = RocksDBProvider.initRockDB(dbFile, version, conf);
    managedDb.close();
    // A second close must not throw. The recovery path can race with explicit close in production,
    // so this safety is load-bearing.
    managedDb.close();
  }

  @Test
  public void testCloseWithoutSetDbReleasesPartialInitResources() {
    // Simulates the partial-init state inside RocksDBProvider when RocksDB.open(...) fails
    // before setDb is ever called: the four open-time resources are allocated, but no DB
    // handle has been attached. close() must still release them and must not NPE on the
    // null db reference.
    Options dbOptions = new Options();
    BloomFilter bloomFilter = new BloomFilter(10.0D, false);
    org.rocksdb.Logger rocksDBLogger =
        new org.rocksdb.Logger(dbOptions.infoLogLevel()) {
          @Override
          protected void log(InfoLogLevel infoLogLevel, String logMsg) {}
        };

    assertTrue(dbOptions.isOwningHandle());
    assertTrue(bloomFilter.isOwningHandle());
    assertTrue(rocksDBLogger.isOwningHandle());

    ManagedRocksDB managedDb = new ManagedRocksDB(dbOptions, bloomFilter, rocksDBLogger);
    assertNull("db should be unset for the partial-init case", managedDb.db());

    managedDb.close();

    assertFalse(
        "Options must be released even when db was never attached", dbOptions.isOwningHandle());
    assertFalse(
        "BloomFilter must be released even when db was never attached",
        bloomFilter.isOwningHandle());
    assertFalse(
        "Logger must be released even when db was never attached", rocksDBLogger.isOwningHandle());
  }

  @Test
  public void testDefaultCompressionMatchesPreviousHardcodedValues() throws Exception {
    ManagedRocksDB managedDb = RocksDBProvider.initRockDB(dbFile, version, conf);
    try {
      assertEquals(CompressionType.LZ4_COMPRESSION, managedDb.options().compressionType());
      assertEquals(
          CompressionType.ZSTD_COMPRESSION, managedDb.options().bottommostCompressionType());
    } finally {
      managedDb.close();
    }
  }

  @Test
  public void testCustomCompressionTypeFromConf() throws Exception {
    conf.set("celeborn.worker.graceful.shutdown.recoverDb.rocksdb.compression", "NO_COMPRESSION");
    conf.set(
        "celeborn.worker.graceful.shutdown.recoverDb.rocksdb.bottommostCompression",
        "SNAPPY_COMPRESSION");
    ManagedRocksDB managedDb = RocksDBProvider.initRockDB(dbFile, version, conf);
    try {
      assertEquals(CompressionType.NO_COMPRESSION, managedDb.options().compressionType());
      assertEquals(
          CompressionType.SNAPPY_COMPRESSION, managedDb.options().bottommostCompressionType());
    } finally {
      managedDb.close();
    }
  }

  @Test
  public void testCompressionTypeIsCaseInsensitive() throws Exception {
    conf.set("celeborn.worker.graceful.shutdown.recoverDb.rocksdb.compression", "zstd_compression");
    ManagedRocksDB managedDb = RocksDBProvider.initRockDB(dbFile, version, conf);
    try {
      assertEquals(CompressionType.ZSTD_COMPRESSION, managedDb.options().compressionType());
    } finally {
      managedDb.close();
    }
  }

  @Test
  public void testCustomBloomFilterBitsPerKeyOpensCleanly() throws Exception {
    // Bloom filter bits-per-key is wrapped inside BlockBasedTableConfig and not directly
    // readable back via the RocksDB Java API, so the practical assertion is that the DB
    // builds and opens without throwing when a non-default value is supplied.
    conf.set("celeborn.worker.graceful.shutdown.recoverDb.rocksdb.bloomFilter.bitsPerKey", "15.0");
    ManagedRocksDB managedDb = RocksDBProvider.initRockDB(dbFile, version, conf);
    try {
      assertNotNull(managedDb.db());
      byte[] key = "k".getBytes(StandardCharsets.UTF_8);
      byte[] value = "v".getBytes(StandardCharsets.UTF_8);
      managedDb.put(key, value);
      assertArrayEquals(value, managedDb.get(key));
    } finally {
      managedDb.close();
    }
  }
}
