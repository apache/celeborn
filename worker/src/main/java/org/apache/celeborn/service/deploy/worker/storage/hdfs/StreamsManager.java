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

package org.apache.celeborn.service.deploy.worker.storage.hdfs;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.service.deploy.worker.WorkerSource;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

// A concurrent HDFS stream map implementation with the ability to safely cleanup particular entry from the map
// in parallel.
public final class StreamsManager {
    public static final Logger log = LoggerFactory.getLogger(StreamsManager.class);

    private final CelebornConf conf;
    private final ConcurrentMap<Path, AspEntry> hdfsStreamByPartitionFilePath;
    private final AbstractSource workerSource;

    private final FileSystem fileSystem;

    private final ScheduledExecutorService executorService;

    private final int maxCapacity;

    private final long maxStreamIdleMs;
    public StreamsManager(CelebornConf conf, AbstractSource workerSource, FileSystem fileSystem) {
        this.conf = conf;
        this.workerSource = workerSource;
        this.fileSystem = fileSystem;
        this.maxCapacity = conf.workerOpenHDFSOutputStreamMax();
        this.hdfsStreamByPartitionFilePath = new ConcurrentHashMap<>(this.maxCapacity);
        this.maxStreamIdleMs = conf.workerHDFSOutputStreamIdleMsMax();
        this.executorService = Executors.newScheduledThreadPool(2, new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("CleanUpStreamMap")
                .build()
        );
        this.executorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                cleanup();
            }
        }, 1, 3, TimeUnit.SECONDS);
    }

    public int getSize() {
        return hdfsStreamByPartitionFilePath.size();
    }

    AspEntry maybeDropEntry(AspEntry expectedEntry, AspEntry currentEntry) {
        if (expectedEntry == currentEntry) {
            // Here we cannot call shouldCleanup otherwise it might not be able to clean up.
            if (!expectedEntry.isInUse() || System.currentTimeMillis() - expectedEntry.lastActiveTimeMs.get() > this.maxStreamIdleMs) {
                // Should be cleaned up.
                return null;
            }
            log.info("Try to drop entry with file path {}, which is still in use.",
                    expectedEntry.filePath);
        }
        return currentEntry;
    }

    boolean cleanUpAspEntry(Path path, AspEntry aspEntry) {
        AspEntry newEntry = hdfsStreamByPartitionFilePath.computeIfPresent(
                path, (k, ov) -> maybeDropEntry(aspEntry, ov));
        if (newEntry == null) {
            // Entry is dropped or there is no such entry
            if (aspEntry != null) {
                cleanupSafely(aspEntry);
                hdfsStreamByPartitionFilePath.remove(path);
                return true;
            }
        }
        return false;
    }

    public void cleanup() {
        log.info("Current active streams before cleaning up {}", hdfsStreamByPartitionFilePath.size());
        int numCleanups = 0;
        for (Map.Entry<Path, AspEntry> entry : hdfsStreamByPartitionFilePath.entrySet()) {
            AspEntry aspEntry = entry.getValue();
            if (aspEntry != null && aspEntry.shouldCleanup(System.currentTimeMillis(), maxStreamIdleMs)) {
                if (cleanUpAspEntry(entry.getKey(), aspEntry)) {
                    ++numCleanups;
                }
            }
        }
        if (numCleanups > 0) {
            log.info("Cleaned up HDFS {} streams.", numCleanups);
        }
    }

    public void cleanup(Set<String> set) {
        if (set == null || set.size() == 0) {
            return;
        }
        Set<String> removeSet = new HashSet<>();
        for (String str : set) {
            removeSet.add(str.replace('-', '/'));
        }
        int numCleanups = 0;
        for (Map.Entry<Path, AspEntry> entry : hdfsStreamByPartitionFilePath.entrySet()) {
            AspEntry aspEntry = entry.getValue();
            if (aspEntry != null) {
                for (String str : removeSet) {
                    if (entry.getKey().toString().contains(str)) {
                        cleanupSafely(aspEntry);
                        hdfsStreamByPartitionFilePath.remove(entry.getKey());
                        ++numCleanups;
                    }
                }
            }
        }
        if (numCleanups > 0) {
            log.info("Cleaned up HDFS {} streams.", numCleanups);
        }
    }

    void cleanupSafely(AspEntry aspEntry) {
        try {
            if (aspEntry != null) {
                aspEntry.close();
            }
        } catch (IOException e) {
            log.error("Got exception during FSDataOutputStream.close {}.", aspEntry.filePath);
        }
    }

    // Returns whether the entry for the given asp has been dropped.
    public boolean dropEntry(Path path) {
        final AspEntry entry = hdfsStreamByPartitionFilePath.get(path);
        if (entry != null) {
            return cleanUpAspEntry(path, entry);
        } else {
            return true;
        }
    }

    public AspEntry addStream(Path shuffleFile) throws IOException {
        long nowMs = System.currentTimeMillis();
        AspEntry newEntry = new AspEntry(shuffleFile, nowMs);
        FSDataOutputStream outputStream = null;
        if (fileSystem.exists(shuffleFile)) {
            int retryCount = 0;
            while (retryCount < conf.workerGetStreamMaxAttempts() && outputStream == null) {
                try {
                    outputStream = fileSystem.append(shuffleFile);
                } catch (IOException e) {
                    log.error("Failed to append to file {}.", shuffleFile, e);
                    retryCount++;
                    if (retryCount < conf.workerGetStreamMaxAttempts()) {
                        try {
                            Thread.sleep(300);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            }
        } else {
            int retryCount = 0;
            while (retryCount < conf.workerGetStreamMaxAttempts() && outputStream == null) {
                try {
                    outputStream = fileSystem.create(shuffleFile);
                } catch (IOException e) {
                    log.error("Failed to create to file {}.", shuffleFile, e);
                    retryCount++;
                    if (retryCount < conf.workerGetStreamMaxAttempts()) {
                        try {
                            Thread.sleep(300);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            }
        }
        newEntry.unsafeSetOutputStream(outputStream);
        log.debug("Successfully created a AspEntry for {}", shuffleFile);
        return hdfsStreamByPartitionFilePath.putIfAbsent(shuffleFile, newEntry);
    }

    // If the entry does not exist, create a new one and insert it to the map.
    public AspEntry getOrCreateStream(Path path) throws IOException {
        final long startMs = System.currentTimeMillis();
        AspEntry entry = hdfsStreamByPartitionFilePath.computeIfPresent(
                path, (k, ov) -> {
                    ov.acquireStream(startMs);
                    return ov;
                });
        if (entry != null && entry.canReuseOutputStream()) {
            workerSource.incCounter(WorkerSource.REUSE_HDFS_OUTPUT_STREAM_TOTAL_COUNT());
            return entry;
        }
        entry = addStream(path);
        return entry;
    }

    public static final class AspEntry {
        private final AtomicLong lastActiveTimeMs;
        private final AtomicBoolean inUse;
        final Path filePath;
        private final AtomicReference<FSDataOutputStream> outputStreamRef;

        AspEntry(Path path, long nowMs) {
            this.filePath = path;
            this.outputStreamRef = new AtomicReference(null);
            lastActiveTimeMs = new AtomicLong(nowMs);
            inUse = new AtomicBoolean(false);
        }

        void unsafeSetOutputStream(FSDataOutputStream outputStream) {
            if (outputStream == null) {
                throw new IllegalArgumentException("A Null recordWriter for " + filePath);
            }
            this.outputStreamRef.getAndSet(outputStream);
        }

        boolean canReuseOutputStream() {
            return outputStreamRef.get() != null;
        }
        public FSDataOutputStream getFSDataOutputStream() {
            final FSDataOutputStream outputStream = this.outputStreamRef.get();
            if (outputStream == null) {
                throw new IllegalArgumentException("A Null outputStream for " + filePath);
            }
            return outputStream;
        }

        private void setActiveTime(long nowMs) {
            long lastTime = lastActiveTimeMs.get();
            if (lastTime < nowMs) {
                lastActiveTimeMs.compareAndSet(lastTime, nowMs);
            }
        }
        void acquireStream(long nowMs) {
            inUse.set(true);
            setActiveTime(nowMs);
        }

        public void releaseStream() {
            setActiveTime(System.currentTimeMillis());
            inUse.set(false);
        }

        boolean isInUse() {
            return inUse.get();
        }

        boolean shouldCleanup(long nowMs, long maxInactiveTimeMs) {
            boolean res = !isInUse() && (lastActiveTimeMs.get() + maxInactiveTimeMs < nowMs);
            return res;
        }

        void close() throws IOException {
            synchronized (outputStreamRef) {
                if (!isInUse()) {
                    if (outputStreamRef.get() != null) {
                        IOUtils.closeStream(outputStreamRef.get());
                    }
                    outputStreamRef.getAndSet(null);
                }
            }
        }
    }
}