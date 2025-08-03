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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public final class StreamsManager {
    public static final Logger log = LoggerFactory.getLogger(StreamsManager.class);

    LoadingCache<Path, FSDataOutputStream> cache;
    private final CelebornConf conf;
    private final FileSystem fileSystem;
    private final int maxCapacity;
    private final long maxStreamIdleMs;
    private long totalGetkeyCount = 0;
    private long totalLoaderCount = 0;
    private final long maxCount = 1000000000000L;

    private final int concurrentLevel;
    public StreamsManager(CelebornConf conf, FileSystem fileSystem) {
        this.conf = conf;
        this.fileSystem = fileSystem;
        this.maxCapacity = conf.workerOpenHDFSOutputStreamMax();
        this.maxStreamIdleMs = conf.workerHDFSOutputStreamIdleMsMax();
        this.concurrentLevel = conf.workerHDFSOutputStreamConcurrentLevel();
        RemovalListener<Path, FSDataOutputStream> listener = new RemovalListener<Path, FSDataOutputStream>() {
            @Override
            public void onRemoval(RemovalNotification<Path, FSDataOutputStream> notification) {
                if (notification.getValue() != null) {
                    try {
                        notification.getValue().close();
                        Thread.sleep(20);
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
        CacheLoader<Path, FSDataOutputStream> cacheLoader = new CacheLoader<>() {
            @SuppressWarnings("NullableProblems")
            @Override
            public FSDataOutputStream load(Path path) throws Exception {
                totalLoaderCount++;
                try {
                    return fileSystem.append(path);
                } catch (IOException e){
                    throw new IOException("File must be exist: " + path + "\n" + e);
                }
            }
        };
        cache = CacheBuilder.newBuilder()
                .maximumSize(this.maxCapacity)
                .concurrencyLevel(this.concurrentLevel)
                .expireAfterAccess(this.maxStreamIdleMs, TimeUnit.MILLISECONDS)
                .removalListener(listener)
                .build(cacheLoader);
    }

    public long getSize() {
        return cache.size();
    }

    public synchronized void dropEntry(Path path) {
        FSDataOutputStream outputStream = cache.getIfPresent(path);
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            cache.invalidate(path);
        }
    }

    public long getReuseOutputStreamCount() {
        long res = totalGetkeyCount - totalLoaderCount;
        if (totalGetkeyCount > maxCount || totalLoaderCount > maxCount) {
            totalGetkeyCount = 0;
            totalLoaderCount = 0;
        }
        return res;
    }

    public synchronized FSDataOutputStream getOrCreateStream(Path path) {
        totalGetkeyCount++;
        try {
            return cache.get(path);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}