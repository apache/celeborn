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

package org.apache.celeborn.service.deploy.worker.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.*;
import org.apache.celeborn.common.util.ShuffleBlockInfoUtils.ShuffleBlockInfo;
import org.apache.celeborn.service.deploy.worker.ShuffleRecoverHelper;
import org.apache.celeborn.service.deploy.worker.WorkerSource;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;
import org.apache.celeborn.service.deploy.worker.shuffledb.DB;
import org.apache.celeborn.service.deploy.worker.shuffledb.DBBackend;
import org.apache.celeborn.service.deploy.worker.shuffledb.DBIterator;
import org.apache.celeborn.service.deploy.worker.shuffledb.DBProvider;
import org.apache.celeborn.service.deploy.worker.shuffledb.StoreVersion;

public class PartitionFilesSorter extends ShuffleRecoverHelper {
  private static final Logger logger = LoggerFactory.getLogger(PartitionFilesSorter.class);

  private static final StoreVersion CURRENT_VERSION = new StoreVersion(1, 0);
  private static final String RECOVERY_SORTED_FILES_FILE_NAME_PREFIX = "sortedFiles";
  private File recoverFile;
  private volatile boolean shutdown = false;
  private final ConcurrentHashMap<String, Set<String>> sortedShuffleFiles =
      JavaUtils.newConcurrentHashMap();
  private final ConcurrentHashMap<String, Set<String>> sortingShuffleFiles =
      JavaUtils.newConcurrentHashMap();
  private final Cache<String, Map<Integer, List<ShuffleBlockInfo>>> indexCache;
  private final Map<String, Set<String>> indexCacheNames = JavaUtils.newConcurrentHashMap();

  private final LinkedBlockingQueue<FileSorter> shuffleSortTaskDeque = new LinkedBlockingQueue<>();
  private final PartitionFilesCleaner cleaner;

  private final AtomicInteger sortedFileCount = new AtomicInteger();
  private final AtomicLong sortedFilesSize = new AtomicLong();
  protected final long sortTimeout;
  protected final long shuffleChunkSize;
  protected final long reservedMemoryPerPartition;
  private boolean gracefulShutdown;
  private long partitionSorterShutdownAwaitTime;
  private DB sortedFilesDb;

  protected final AbstractSource source;

  private final ExecutorService fileSorterExecutors;
  private final ExecutorService fileSorterSchedulerThread;
  private final long indexCacheMaxWeight;

  public PartitionFilesSorter(
      MemoryManager memoryManager, CelebornConf conf, AbstractSource source) {
    this.sortTimeout = conf.partitionSorterSortPartitionTimeout();
    this.shuffleChunkSize = conf.shuffleChunkSize();
    this.reservedMemoryPerPartition = conf.partitionSorterReservedMemoryPerPartition();
    this.partitionSorterShutdownAwaitTime =
        conf.workerGracefulShutdownPartitionSorterCloseAwaitTimeMs();
    this.indexCacheMaxWeight = conf.partitionSorterIndexCacheMaxWeight();
    this.source = source;
    this.cleaner = new PartitionFilesCleaner(this);
    this.gracefulShutdown = conf.workerGracefulShutdown();
    // ShuffleClient can fetch shuffle data from a restarted worker only
    // when the worker's fetching port is stable and enables graceful shutdown.
    if (gracefulShutdown) {
      try {
        String recoverPath = conf.workerGracefulShutdownRecoverPath();
        DBBackend dbBackend = DBBackend.byName(conf.workerGracefulShutdownRecoverDbBackend());
        String recoverySortedFilesFileName =
            dbBackend.fileName(RECOVERY_SORTED_FILES_FILE_NAME_PREFIX);
        this.recoverFile = new File(recoverPath, recoverySortedFilesFileName);
        this.sortedFilesDb = DBProvider.initDB(dbBackend, recoverFile, CURRENT_VERSION);
        reloadAndCleanSortedShuffleFiles(this.sortedFilesDb);
      } catch (Exception e) {
        logger.error("Failed to reload DB for sorted shuffle files from: " + recoverFile, e);
        this.sortedFilesDb = null;
      }
    } else {
      this.sortedFilesDb = null;
    }

    fileSorterExecutors =
        ThreadUtils.newDaemonCachedThreadPool(
            "worker-file-sorter-executor", conf.partitionSorterThreads(), 120);

    indexCache =
        CacheBuilder.newBuilder()
            .concurrencyLevel(conf.partitionSorterThreads())
            .expireAfterAccess(conf.partitionSorterIndexExpire(), TimeUnit.MILLISECONDS)
            .maximumWeight(indexCacheMaxWeight)
            .weigher(
                (key, cache) ->
                    ((Map<Integer, List<ShuffleBlockInfo>>) cache)
                        .values().stream().mapToInt(List::size).sum())
            .build();

    fileSorterSchedulerThread =
        ThreadUtils.newDaemonSingleThreadExecutor("worker-file-sorter-scheduler");
    fileSorterSchedulerThread.submit(
        () -> {
          try {
            while (!shutdown) {
              FileSorter task = shuffleSortTaskDeque.take();
              memoryManager.reserveSortMemory(reservedMemoryPerPartition);
              while (!memoryManager.sortMemoryReady()) {
                Thread.sleep(20);
              }
              fileSorterExecutors.submit(
                  () -> {
                    try {
                      task.sort();
                    } catch (InterruptedException e) {
                      logger.warn("File sorter thread was interrupted.");
                    } finally {
                      memoryManager.releaseSortMemory(reservedMemoryPerPartition);
                    }
                  });
            }
          } catch (InterruptedException e) {
            logger.warn("Sort scheduler thread is shutting down, detail: ", e);
          }
        });
  }

  public int getSortingCount() {
    return shuffleSortTaskDeque.size();
  }

  public int getSortedCount() {
    return sortedFileCount.get();
  }

  public long getSortedSize() {
    return sortedFilesSize.get();
  }

  // return the sorted FileInfo.
  // 1. If the sorted file is not generated, it adds the FileSorter task to the sorting queue and
  //    synchronously waits for the sorted FileInfo.
  // 2. If the FileSorter task is already in the sorting queue but the sorted file has not been
  //    generated, it awaits until a timeout occurs (default 220 seconds).
  // 3. If the sorted file is generated, it returns the sorted FileInfo.
  public FileInfo getSortedFileInfo(
      String shuffleKey, String fileName, FileInfo fileInfo, int startMapIndex, int endMapIndex)
      throws IOException {
    String fileId = shuffleKey + "-" + fileName;
    UserIdentifier userIdentifier = fileInfo.getUserIdentifier();
    Set<String> sorted =
        sortedShuffleFiles.computeIfAbsent(shuffleKey, v -> ConcurrentHashMap.newKeySet());
    Set<String> sorting =
        sortingShuffleFiles.computeIfAbsent(shuffleKey, v -> ConcurrentHashMap.newKeySet());

    String sortedFilePath = Utils.getSortedFilePath(fileInfo.getFilePath());
    String indexFilePath = Utils.getIndexFilePath(fileInfo.getFilePath());
    synchronized (sorting) {
      if (sorted.contains(fileId)) {
        return resolve(
            shuffleKey,
            fileId,
            userIdentifier,
            sortedFilePath,
            indexFilePath,
            startMapIndex,
            endMapIndex);
      }
      if (!sorting.contains(fileId)) {
        try {
          FileSorter fileSorter = new FileSorter(fileInfo, fileId, shuffleKey);
          sorting.add(fileId);
          logger.debug(
              "Adding sorter to sort queue shuffle key {}, file name {}", shuffleKey, fileName);
          shuffleSortTaskDeque.put(fileSorter);
        } catch (InterruptedException e) {
          logger.error("Sorter scheduler thread is interrupted means worker is shutting down.", e);
          throw new IOException(
              "Sort scheduler thread is interrupted means worker is shutting down.", e);
        } catch (IOException e) {
          logger.error("File sorter access HDFS failed.", e);
          throw new IOException("File sorter access HDFS failed.", e);
        }
      }
    }

    long sortStartTime = System.currentTimeMillis();
    while (!sorted.contains(fileId)) {
      if (sorting.contains(fileId)) {
        try {
          Thread.sleep(50);
          if (System.currentTimeMillis() - sortStartTime > sortTimeout) {
            logger.error("Sorting file {} timeout after {}ms", fileId, sortTimeout);
            throw new IOException(
                "Sort file " + fileInfo.getFilePath() + " timeout after " + sortTimeout);
          }
        } catch (InterruptedException e) {
          logger.error("Sorter scheduler thread is interrupted means worker is shutting down.", e);
          throw new IOException(
              "Sorter scheduler thread is interrupted means worker is shutting down.", e);
        }
      } else {
        logger.debug("Sorting shuffle file for {} {} failed.", shuffleKey, fileInfo.getFilePath());
        throw new IOException(
            "Sorting shuffle file for " + shuffleKey + " " + fileInfo.getFilePath() + " failed.");
      }
    }

    return resolve(
        shuffleKey,
        fileId,
        userIdentifier,
        sortedFilePath,
        indexFilePath,
        startMapIndex,
        endMapIndex);
  }

  public void cleanup(HashSet<String> expiredShuffleKeys) {
    for (String expiredShuffleKey : expiredShuffleKeys) {
      sortingShuffleFiles.remove(expiredShuffleKey);
      deleteSortedShuffleFiles(expiredShuffleKey);
      Set<String> expiredIndexCacheItems = indexCacheNames.remove(expiredShuffleKey);
      if (expiredIndexCacheItems != null) {
        for (String expiredIndexCacheItem : expiredIndexCacheItems) {
          indexCache.invalidate(expiredIndexCacheItem);
        }
      }
    }
  }

  public void close(int exitKind) {
    logger.info("Closing {}", this.getClass().getSimpleName());
    shutdown = true;
    if (exitKind == CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN()) {
      long start = System.currentTimeMillis();
      try {
        fileSorterExecutors.shutdown();
        fileSorterExecutors.awaitTermination(
            partitionSorterShutdownAwaitTime, TimeUnit.MILLISECONDS);
        if (!fileSorterExecutors.isShutdown()) {
          fileSorterExecutors.shutdownNow();
        }
      } catch (InterruptedException e) {
        logger.error("Await partition sorter executor shutdown catch exception: ", e);
      }
      if (sortedFilesDb != null) {
        try {
          updateSortedShuffleFilesInDB();
          sortedFilesDb.close();
        } catch (IOException e) {
          logger.error("Store recover data to DB failed.", e);
        }
      }
      long end = System.currentTimeMillis();
      logger.info("Await partition sorter executor complete cost " + (end - start) + "ms");
    } else {
      fileSorterSchedulerThread.shutdownNow();
      fileSorterExecutors.shutdownNow();
      cleaner.close();
      if (sortedFilesDb != null) {
        try {
          sortedFilesDb.close();
          recoverFile.delete();
        } catch (IOException e) {
          logger.error("Clean DB failed.", e);
        }
      }
    }
    indexCache.invalidateAll();
  }

  private void reloadAndCleanSortedShuffleFiles(DB db) {
    if (db != null) {
      DBIterator itr = db.iterator();
      itr.seek(SHUFFLE_KEY_PREFIX.getBytes(StandardCharsets.UTF_8));
      while (itr.hasNext()) {
        Map.Entry<byte[], byte[]> entry = itr.next();
        String key = new String(entry.getKey(), StandardCharsets.UTF_8);
        if (key.startsWith(SHUFFLE_KEY_PREFIX)) {
          String shuffleKey = parseDbShuffleKey(key);
          try {
            Set<String> sortedFiles = PbSerDeUtils.fromPbSortedShuffleFileSet(entry.getValue());
            logger.debug("Reload DB: {} -> {}", shuffleKey, sortedFiles);
            sortedShuffleFiles.put(shuffleKey, sortedFiles);
            sortedFilesDb.delete(entry.getKey());
          } catch (Exception exception) {
            logger.error("Reload DB: {} failed.", shuffleKey, exception);
          }
        }
      }
    }
  }

  @VisibleForTesting
  public void updateSortedShuffleFilesInDB() {
    for (String shuffleKey : sortedShuffleFiles.keySet()) {
      try {
        sortedFilesDb.put(
            dbShuffleKey(shuffleKey),
            PbSerDeUtils.toPbSortedShuffleFileSet(sortedShuffleFiles.get(shuffleKey)));
        logger.debug("Update DB: {} -> {}", shuffleKey, sortedShuffleFiles.get(shuffleKey));
      } catch (Exception exception) {
        logger.error("Update DB: {} failed.", shuffleKey, exception);
      }
    }
  }

  @VisibleForTesting
  public Set<String> initSortedShuffleFiles(String shuffleKey) {
    return sortedShuffleFiles.computeIfAbsent(shuffleKey, v -> ConcurrentHashMap.newKeySet());
  }

  @VisibleForTesting
  public void updateSortedShuffleFiles(String shuffleKey, String fileId, long fileLength) {
    sortedShuffleFiles.get(shuffleKey).add(fileId);
    sortedFileCount.incrementAndGet();
    sortedFilesSize.addAndGet(fileLength);
  }

  @VisibleForTesting
  public void deleteSortedShuffleFiles(String expiredShuffleKey) {
    sortedShuffleFiles.remove(expiredShuffleKey);
  }

  @VisibleForTesting
  public Set<String> getSortedShuffleFiles(String shuffleKey) {
    return sortedShuffleFiles.get(shuffleKey);
  }

  protected void writeIndex(
      Map<Integer, List<ShuffleBlockInfo>> indexMap, String indexFilePath, boolean isHdfs)
      throws IOException {
    FSDataOutputStream hdfsIndexOutput = null;
    FileChannel indexFileChannel = null;
    if (isHdfs) {
      // If the index file exists, it will be overwritten.
      // So there is no need to check its existence.
      hdfsIndexOutput = StorageManager.hadoopFs().create(new Path(indexFilePath));
    } else {
      indexFileChannel = FileChannelUtils.createWritableFileChannel(indexFilePath);
    }

    int indexSize = 0;
    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : indexMap.entrySet()) {
      indexSize += 8;
      indexSize += entry.getValue().size() * 16;
    }

    ByteBuffer indexBuf = ByteBuffer.allocate(indexSize);
    for (Map.Entry<Integer, List<ShuffleBlockInfo>> entry : indexMap.entrySet()) {
      int mapId = entry.getKey();
      List<ShuffleBlockInfo> list = entry.getValue();
      indexBuf.putInt(mapId);
      indexBuf.putInt(list.size());
      list.forEach(
          info -> {
            indexBuf.putLong(info.offset);
            indexBuf.putLong(info.length);
          });
    }

    indexBuf.flip();
    if (isHdfs) {
      // Direct byte buffer has no array, so can not invoke indexBuf.array() here.
      byte[] tmpBuf = new byte[indexSize];
      indexBuf.get(tmpBuf);
      hdfsIndexOutput.write(tmpBuf);
      hdfsIndexOutput.close();
    } else {
      while (indexBuf.hasRemaining()) {
        indexFileChannel.write(indexBuf);
      }
      indexFileChannel.close();
    }
  }

  protected void readStreamFully(FSDataInputStream stream, ByteBuffer buffer, String path)
      throws IOException {
    while (buffer.hasRemaining()) {
      if (-1 == stream.read(buffer)) {
        throw new IOException(
            "Unexpected EOF, file name : "
                + path
                + " position :"
                + stream.getPos()
                + " buffer size :"
                + buffer.limit());
      }
    }
  }

  protected void readChannelFully(FileChannel channel, ByteBuffer buffer, String path)
      throws IOException {
    while (buffer.hasRemaining()) {
      if (-1 == channel.read(buffer)) {
        throw new IOException(
            "Unexpected EOF, file name : "
                + path
                + " position :"
                + channel.position()
                + " buffer size :"
                + buffer.limit());
      }
    }
  }

  private long transferStreamFully(
      FSDataInputStream origin, FSDataOutputStream sorted, long offset, long length)
      throws IOException {
    // Worker read a shuffle block whose size is 256K by default.
    // So there is no need to worry about integer overflow.
    byte[] buffer = new byte[Math.toIntExact(length)];
    origin.readFully(offset, buffer);
    sorted.write(buffer);
    return length;
  }

  private long transferChannelFully(
      FileChannel originChannel, FileChannel targetChannel, long offset, long length)
      throws IOException {
    long transferredSize = 0;
    while (transferredSize != length) {
      transferredSize +=
          originChannel.transferTo(
              offset + transferredSize, length - transferredSize, targetChannel);
    }
    return transferredSize;
  }

  public FileInfo resolve(
      String shuffleKey,
      String fileId,
      UserIdentifier userIdentifier,
      String sortedFilePath,
      String indexFilePath,
      int startMapIndex,
      int endMapIndex)
      throws IOException {
    Map<Integer, List<ShuffleBlockInfo>> indexMap;
    try {
      indexMap =
          indexCache.get(
              fileId,
              () -> {
                FileChannel indexChannel = null;
                FSDataInputStream hdfsIndexStream = null;
                boolean isHdfs = Utils.isHdfsPath(indexFilePath);
                int indexSize = 0;
                try {
                  if (isHdfs) {
                    hdfsIndexStream = StorageManager.hadoopFs().open(new Path(indexFilePath));
                    indexSize =
                        (int)
                            StorageManager.hadoopFs()
                                .getFileStatus(new Path(indexFilePath))
                                .getLen();
                  } else {
                    indexChannel = FileChannelUtils.openReadableFileChannel(indexFilePath);
                    File indexFile = new File(indexFilePath);
                    indexSize = (int) indexFile.length();
                  }
                  ByteBuffer indexBuf = ByteBuffer.allocate(indexSize);
                  if (isHdfs) {
                    readStreamFully(hdfsIndexStream, indexBuf, indexFilePath);
                  } else {
                    readChannelFully(indexChannel, indexBuf, indexFilePath);
                  }
                  indexBuf.rewind();
                  Map<Integer, List<ShuffleBlockInfo>> tIndexMap =
                      ShuffleBlockInfoUtils.parseShuffleBlockInfosFromByteBuffer(indexBuf);
                  Set<String> indexCacheItemsSet =
                      indexCacheNames.computeIfAbsent(shuffleKey, v -> new HashSet<>());
                  indexCacheItemsSet.add(fileId);
                  return tIndexMap;
                } catch (Exception e) {
                  logger.error(
                      "Read sorted shuffle file index " + indexFilePath + " error, detail: ", e);
                  throw new IOException("Read sorted shuffle file index failed.", e);
                } finally {
                  IOUtils.closeQuietly(indexChannel, null);
                  IOUtils.closeQuietly(hdfsIndexStream, null);
                }
              });
    } catch (ExecutionException e) {
      throw new IOException("Read sorted shuffle file index failed.", e);
    }
    return new FileInfo(
        sortedFilePath,
        ShuffleBlockInfoUtils.getChunkOffsetsFromShuffleBlockInfos(
            startMapIndex, endMapIndex, shuffleChunkSize, indexMap),
        userIdentifier);
  }

  class FileSorter {
    private final String originFilePath;
    private final String sortedFilePath;
    private final String indexFilePath;
    private final long originFileLen;
    private final String fileId;
    private final String shuffleKey;
    private final boolean isHdfs;
    private final FileInfo originFileInfo;

    private FSDataInputStream hdfsOriginInput = null;
    private FSDataOutputStream hdfsSortedOutput = null;
    private FileChannel originFileChannel = null;
    private FileChannel sortedFileChannel = null;

    FileSorter(FileInfo fileInfo, String fileId, String shuffleKey) throws IOException {
      this.originFileInfo = fileInfo;
      this.originFilePath = fileInfo.getFilePath();
      this.sortedFilePath = Utils.getSortedFilePath(originFilePath);
      this.isHdfs = fileInfo.isHdfs();
      this.originFileLen = fileInfo.getFileLength();
      this.fileId = fileId;
      this.shuffleKey = shuffleKey;
      this.indexFilePath = Utils.getIndexFilePath(originFilePath);
      if (!isHdfs) {
        File sortedFile = new File(this.sortedFilePath);
        if (sortedFile.exists()) {
          sortedFile.delete();
        }
        File indexFile = new File(this.indexFilePath);
        if (indexFile.exists()) {
          indexFile.delete();
        }
      } else {
        if (StorageManager.hadoopFs().exists(fileInfo.getHdfsSortedPath())) {
          StorageManager.hadoopFs().delete(fileInfo.getHdfsSortedPath(), false);
        }
        if (StorageManager.hadoopFs().exists(fileInfo.getHdfsIndexPath())) {
          StorageManager.hadoopFs().delete(fileInfo.getHdfsIndexPath(), false);
        }
      }
    }

    public void sort() throws InterruptedException {
      source.startTimer(WorkerSource.SORT_TIME(), fileId);

      try {
        initializeFiles();

        Map<Integer, List<ShuffleBlockInfo>> originShuffleBlockInfos = new TreeMap<>();
        Map<Integer, List<ShuffleBlockInfo>> sortedBlockInfoMap = new HashMap<>();

        int batchHeaderLen = 16;
        int reserveMemory = (int) reservedMemoryPerPartition;
        ByteBuffer headerBuf = ByteBuffer.allocate(batchHeaderLen);
        ByteBuffer paddingBuf = ByteBuffer.allocateDirect(reserveMemory);

        long index = 0;
        while (index != originFileLen) {
          long blockStartIndex = index;
          readBufferFully(headerBuf);
          byte[] batchHeader = headerBuf.array();
          headerBuf.rewind();

          int mapId = Platform.getInt(batchHeader, Platform.BYTE_ARRAY_OFFSET);
          final int compressedSize = Platform.getInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 12);

          List<ShuffleBlockInfo> singleMapIdShuffleBlockList =
              originShuffleBlockInfos.computeIfAbsent(mapId, v -> new ArrayList<>());
          ShuffleBlockInfo blockInfo = new ShuffleBlockInfo();
          blockInfo.offset = blockStartIndex;
          blockInfo.length = compressedSize + 16L;
          singleMapIdShuffleBlockList.add(blockInfo);

          index += batchHeaderLen + compressedSize;
          paddingBuf.clear();
          readBufferBySize(paddingBuf, compressedSize);
        }

        long fileIndex = 0;
        for (Map.Entry<Integer, List<ShuffleBlockInfo>> originBlockInfoEntry :
            originShuffleBlockInfos.entrySet()) {
          int mapId = originBlockInfoEntry.getKey();
          List<ShuffleBlockInfo> originShuffleBlocks = originBlockInfoEntry.getValue();
          List<ShuffleBlockInfo> sortedShuffleBlocks = new ArrayList<>();
          for (ShuffleBlockInfo blockInfo : originShuffleBlocks) {
            long offset = blockInfo.offset;
            long length = blockInfo.length;
            ShuffleBlockInfo sortedBlock = new ShuffleBlockInfo();
            sortedBlock.offset = fileIndex;
            sortedBlock.length = length;
            sortedShuffleBlocks.add(sortedBlock);
            fileIndex += transferBlock(offset, length);
          }
          sortedBlockInfoMap.put(mapId, sortedShuffleBlocks);
        }

        writeIndex(sortedBlockInfoMap, indexFilePath, isHdfs);
        updateSortedShuffleFiles(shuffleKey, fileId, originFileLen);
        originFileInfo.setSorted();
        cleaner.add(this);
        logger.debug("sort complete for {} {}", shuffleKey, originFilePath);
      } catch (Exception e) {
        logger.error(
            "Sorting shuffle file for " + fileId + " " + originFilePath + " failed, detail: ", e);
      } finally {
        closeFiles();
        Set<String> sorting = sortingShuffleFiles.get(shuffleKey);
        synchronized (sorting) {
          sorting.remove(fileId);
        }
      }
      source.stopTimer(WorkerSource.SORT_TIME(), fileId);
    }

    public String getShuffleKey() {
      return shuffleKey;
    }

    public FileInfo getOriginFileInfo() {
      return originFileInfo;
    }

    private void initializeFiles() throws IOException {
      if (isHdfs) {
        hdfsOriginInput = StorageManager.hadoopFs().open(new Path(originFilePath));
        hdfsSortedOutput =
            StorageManager.hadoopFs().create(new Path(sortedFilePath), true, 256 * 1024);
      } else {
        originFileChannel = FileChannelUtils.openReadableFileChannel(originFilePath);
        sortedFileChannel = FileChannelUtils.createWritableFileChannel(sortedFilePath);
      }
    }

    private void closeFiles() {
      IOUtils.closeQuietly(hdfsOriginInput, null);
      IOUtils.closeQuietly(hdfsSortedOutput, null);
      IOUtils.closeQuietly(originFileChannel, null);
      IOUtils.closeQuietly(sortedFileChannel, null);
    }

    private void readBufferFully(ByteBuffer buffer) throws IOException {
      if (isHdfs) {
        readStreamFully(hdfsOriginInput, buffer, originFilePath);
      } else {
        readChannelFully(originFileChannel, buffer, originFilePath);
      }
    }

    private long transferBlock(long offset, long length) throws IOException {
      if (isHdfs) {
        return transferStreamFully(hdfsOriginInput, hdfsSortedOutput, offset, length);
      } else {
        return transferChannelFully(originFileChannel, sortedFileChannel, offset, length);
      }
    }

    public void deleteOriginFiles() throws IOException {
      boolean deleteSuccess = false;
      if (isHdfs) {
        deleteSuccess = StorageManager.hadoopFs().delete(new Path(originFilePath), false);
      } else {
        deleteSuccess = new File(originFilePath).delete();
      }
      if (!deleteSuccess) {
        logger.warn("Clean origin file failed, origin file is : {}", originFilePath);
      }
    }

    protected void readChannelBySize(
        FileChannel channel, ByteBuffer buffer, String path, int toRead) throws IOException {
      int read = 0;
      if (toRead < buffer.capacity()) {
        buffer.limit(toRead);
      }
      while (read != toRead) {
        int tmpRead = channel.read(buffer);
        if (-1 == tmpRead) {
          throw new IOException(
              "Unexpected EOF, file name : "
                  + path
                  + " position :"
                  + channel.position()
                  + " read size :"
                  + read);
        } else {
          read += tmpRead;
          if (!buffer.hasRemaining()) {
            buffer.clear();
            if (toRead - read < buffer.capacity()) {
              buffer.limit(toRead - read);
            }
          }
        }
      }
    }

    private void readBufferBySize(ByteBuffer buffer, int toRead) throws IOException {
      if (isHdfs) {
        // HDFS don't need warmup
        hdfsOriginInput.seek(toRead + hdfsOriginInput.getPos());
      } else {
        readChannelBySize(originFileChannel, buffer, originFilePath, toRead);
      }
    }
  }

  public void cleanupExpiredShuffleKey(Set<String> expiredShuffleKeys) {
    cleaner.cleanupExpiredShuffleKey(expiredShuffleKeys);
  }

  public boolean isShutdown() {
    return shutdown;
  }
}

class PartitionFilesCleaner {
  private static final Logger logger = LoggerFactory.getLogger(PartitionFilesCleaner.class);

  private final LinkedBlockingQueue<PartitionFilesSorter.FileSorter> fileSorters =
      new LinkedBlockingQueue<>();
  private final Lock lock = new ReentrantLock();
  private final Condition notEmpty = lock.newCondition();
  private final ExecutorService cleaner =
      ThreadUtils.newDaemonSingleThreadExecutor("worker-partition-file-cleaner");

  PartitionFilesCleaner(PartitionFilesSorter partitionFilesSorter) {
    cleaner.submit(
        () -> {
          try {
            while (!partitionFilesSorter.isShutdown()) {
              lock.lockInterruptibly();
              try {
                // CELEBORN-1210: use while instead of if in case of spurious wakeup.
                while (fileSorters.isEmpty()) {
                  notEmpty.await();
                }
                Iterator<PartitionFilesSorter.FileSorter> it = fileSorters.iterator();
                while (it.hasNext()) {
                  PartitionFilesSorter.FileSorter sorter = it.next();
                  try {
                    if (sorter.getOriginFileInfo().isStreamsEmpty()) {
                      logger.debug(
                          "Deleting the original files for shuffle key {}: {}",
                          sorter.getShuffleKey(),
                          sorter.getOriginFileInfo().getFilePath());
                      sorter.deleteOriginFiles();
                      it.remove();
                    }
                  } catch (IOException e) {
                    logger.error("catch IOException when delete origin files", e);
                  }
                }
              } finally {
                lock.unlock();
              }
            }
          } catch (InterruptedException e) {
            logger.warn("partition file cleaner thread interrupted while wait new sorter.", e);
          }
        });
  }

  public void add(PartitionFilesSorter.FileSorter fileSorter) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      fileSorters.add(fileSorter);
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
  }

  public void cleanupExpiredShuffleKey(Set<String> expiredShuffleKeys) {
    lock.lock();
    try {
      fileSorters.removeIf(sorter -> expiredShuffleKeys.contains(sorter.getShuffleKey()));
    } finally {
      lock.unlock();
    }
  }

  public void close() {
    fileSorters.clear();
    cleaner.shutdownNow();
  }
}
