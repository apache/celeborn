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
import java.util.ArrayList;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.util.PbSerDeUtils;

/**
 * RocksDB utility class available in the network package.
 *
 * <p>Note: code copied from Apache Spark.
 */
public class RocksDBProvider {

  static {
    org.rocksdb.RocksDB.loadLibrary();
  }

  private static final Logger logger = LoggerFactory.getLogger(RocksDBProvider.class);

  public static org.rocksdb.RocksDB initRockDB(File dbFile, StoreVersion version)
      throws IOException {
    org.rocksdb.RocksDB tmpDb = null;
    if (dbFile != null) {
      BloomFilter fullFilter = new BloomFilter(10.0D /* BloomFilter.DEFAULT_BITS_PER_KEY */, false);
      BlockBasedTableConfig tableFormatConfig =
          new BlockBasedTableConfig()
              .setFilterPolicy(fullFilter)
              .setEnableIndexCompression(false)
              .setIndexBlockRestartInterval(8)
              .setFormatVersion(5);

      Options dbOptions = new Options();
      RocksDBLogger rocksDBLogger = new RocksDBLogger(dbOptions);

      dbOptions.setCreateIfMissing(false);
      dbOptions.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
      dbOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
      dbOptions.setTableFormatConfig(tableFormatConfig);
      dbOptions.setLogger(rocksDBLogger);

      try {
        tmpDb = org.rocksdb.RocksDB.open(dbOptions, dbFile.toString());
      } catch (RocksDBException e) {
        if (e.getStatus().getCode() == Status.Code.NotFound) {
          logger.info("Creating state database at " + dbFile);
          createIfMissing(dbOptions, dbFile);
          try {
            tmpDb = org.rocksdb.RocksDB.open(dbOptions, dbFile.toString());
          } catch (RocksDBException dbExc) {
            throw new IOException("Unable to create state store", dbExc);
          }
        } else {
          // the RocksDB file seems to be corrupt somehow.  Let's just blow it away and create
          // a new one, so we can keep processing new apps
          logger.error(
              "Error opening rocksdb file {}. Creating new file, will not be able to "
                  + "recover state for existing applications",
              dbFile,
              e);
          if (dbFile.isDirectory()) {
            for (File f : Objects.requireNonNull(dbFile.listFiles())) {
              if (!f.delete()) {
                logger.warn("Error deleting {}", f.getPath());
              }
            }
          }
          if (!dbFile.delete()) {
            logger.warn("Error deleting {}", dbFile.getPath());
          }
          createIfMissing(dbOptions, dbFile);
          try {
            tmpDb = org.rocksdb.RocksDB.open(dbOptions, dbFile.toString());
          } catch (RocksDBException dbExc) {
            throw new IOException("Unable to create state store", dbExc);
          }
        }
      }
      try {
        // if there is a version mismatch, we throw an exception, which means the service
        // is unusable
        checkVersion(tmpDb, version);
      } catch (RocksDBException e) {
        tmpDb.close();
        throw new IOException(e.getMessage(), e);
      } catch (IOException ioe) {
        tmpDb.close();
        throw ioe;
      }
    }
    return tmpDb;
  }

  private static void createIfMissing(Options dbOptions, File dbFile) {
    logger.info("Creating database file {} if missing", dbFile);
    dbOptions.setCreateIfMissing(true);
    // RocksDB does not support creating non-existent multi-level directory.
    if (!dbFile.exists()) {
      try {
        FileUtils.forceMkdir(dbFile);
      } catch (IOException e) {
        logger.warn("Failed to create database file {}", dbFile, e);
      }
    }
  }

  private static class RocksDBLogger extends org.rocksdb.Logger {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBLogger.class);

    RocksDBLogger(Options options) {
      super(options.infoLogLevel());
    }

    @Override
    protected void log(InfoLogLevel infoLogLevel, String message) {
      if (infoLogLevel == InfoLogLevel.INFO_LEVEL) {
        LOG.info(message);
      }
    }
  }

  /**
   * Simple major.minor versioning scheme. Any incompatible changes should be across major versions.
   * Minor version differences are allowed -- meaning we should be able to read dbs that are either
   * earlier *or* later on the minor version.
   */
  public static void checkVersion(org.rocksdb.RocksDB db, StoreVersion newVersion)
      throws IOException, RocksDBException {
    byte[] bytes = db.get(StoreVersion.KEY);
    if (bytes == null) {
      storeVersion(db, newVersion);
    } else {
      ArrayList<Integer> versions = PbSerDeUtils.fromPbStoreVersion(bytes);
      StoreVersion version = new StoreVersion(versions.get(0), versions.get(1));
      if (version.major != newVersion.major) {
        throw new IOException(
            "Cannot read state DB with version "
                + version
                + ", incompatible "
                + "with current version "
                + newVersion);
      }
      storeVersion(db, newVersion);
    }
  }

  public static void storeVersion(org.rocksdb.RocksDB db, StoreVersion version)
      throws RocksDBException {
    db.put(StoreVersion.KEY, PbSerDeUtils.toPbStoreVersion(version.major, version.minor));
  }
}
