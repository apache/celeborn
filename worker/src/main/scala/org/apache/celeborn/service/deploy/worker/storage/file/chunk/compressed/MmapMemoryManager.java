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

package org.apache.celeborn.service.deploy.worker.storage.file.chunk.compressed;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

public class MmapMemoryManager {
    private static final Logger LOG = Logger.getLogger(MmapMemoryManager.class.getName());
    private static MmapMemoryManager INSTANCE;
    private static final long DEFAULT_FILE_LENGTH = 512 * 1024 * 1024L;
    private final String _dirPathName;
    // _availableOffset has the starting offset for the next allocation in _currentBuffer. When _currentBuffer
    // is created, it is 0. After we allocate a buffer of size x, it is x. And if we allocate another buffer of size
    // y, then it becomes x+y, etc. We try to fulfil as many allocate() calls as possible on the same _currentBuffer
    // until the _currentBuffer cannot hold the new object anymore, and then we create a new _currentBuffer.
    private long _availableOffset = DEFAULT_FILE_LENGTH; // Available offset in this file.
    private long _curFileLen = -1;
    private final List<String> _paths = new LinkedList<>();
    private final List<ByteBuffer> _memMappedBuffers = new LinkedList<>();
    ByteBuffer _currentBuffer;


    public static MmapMemoryManager getInstance() {
        if (INSTANCE == null) {
            synchronized (MmapMemoryManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = createInstance();
                }
            }
        }

        return INSTANCE;
    }

    private static MmapMemoryManager createInstance() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        String dirPathName = tmpDir + "/celeborn-mmap-memory-manager";
        File dirFile = new File(dirPathName);
        if (!dirFile.exists()) {
            if (!dirFile.mkdirs()) {
                throw new RuntimeException("Unable to create directory: " + dirFile);
            }
        }
        return new MmapMemoryManager(dirPathName);
    }

    private MmapMemoryManager(String dirPathName) {
        _dirPathName = dirPathName;
    }

    private String getFilePrefix() {
        return UUID.randomUUID() + ".";
    }

    private void addFileIfNecessary(long len) {
        if (len + _availableOffset <= _curFileLen) {
            return;
        }
        String filePath = _dirPathName + "/" + getFilePrefix();
        final File file = new File(filePath);
        if (file.exists()) {
            throw new RuntimeException("File " + filePath + " already exists");
        }
        file.deleteOnExit();
        long fileLen = Math.max(DEFAULT_FILE_LENGTH, len);
        try (RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
             FileChannel fileChannel = raf.getChannel()) {
            raf.setLength(fileLen);
            _currentBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileLen);
            _memMappedBuffers.add(_currentBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        _paths.add(filePath);
        _availableOffset = 0;
        _curFileLen = fileLen;
    }

    public synchronized ByteBuffer allocateBuffer(long size) {
        addFileIfNecessary(size);
        ByteBuffer buffer = _currentBuffer.duplicate();
        buffer.position((int) _availableOffset);
        buffer.limit((int) (_availableOffset + size));
        _availableOffset += size;
        return buffer.slice();
    }

    protected void close() {
        // MappedByteBuffers cannot be explicitly unmapped in Java; GC handles the unmap.
        // We clear the internal state and delete the backing files so disk space is reclaimed.
        _memMappedBuffers.clear();
        for (String path : _paths) {
            File file = new File(path);
            if (!file.delete()) {
                LOG.warning("Unable to delete mmap backing file: " + file);
            }
        }
        _paths.clear();
        _curFileLen = -1;
        _availableOffset = DEFAULT_FILE_LENGTH;
    }
}
