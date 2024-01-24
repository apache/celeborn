/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.worker.storage.segment;

import io.netty.buffer.ByteBuf;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.protocol.PartitionSplitMode;
import org.apache.celeborn.service.deploy.worker.storage.DeviceMonitor;
import org.apache.celeborn.service.deploy.worker.storage.Flusher;
import org.apache.celeborn.service.deploy.worker.storage.MapPartitionFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SegmentMapPartitionFileWriter extends MapPartitionFileWriter {

    public static final Logger logger = LoggerFactory.getLogger(SegmentMapPartitionFileWriter.class);

    private final Map<Integer, Boolean> subpartitionHasStartSegment;

    private int[] subpartitionBufferIndex;

    private int currentRegionIndex;

    private boolean hasRegionStarted;

    public SegmentMapPartitionFileWriter(
            FileInfo fileInfo,
            Flusher flusher,
            AbstractSource workerSource,
            CelebornConf conf,
            DeviceMonitor deviceMonitor,
            long splitThreshold,
            PartitionSplitMode splitMode,
            boolean rangeReadFilter)
            throws IOException {
        super(
                fileInfo,
                flusher,
                workerSource,
                conf,
                deviceMonitor,
                splitThreshold,
                splitMode,
                rangeReadFilter);
        this.subpartitionHasStartSegment = new HashMap<>();
    }

    @Override
    public void pushDataHandShake(int numSubpartitions, int bufferSize) {
        super.pushDataHandShake(numSubpartitions, bufferSize);
        subpartitionBufferIndex = new int[numSubpartitions];
        Arrays.fill(subpartitionBufferIndex, 0);
    }

    @Override
    public void write(ByteBuf data) throws IOException {
        data.markReaderIndex();
        int partitionId = data.readInt();
        int attemptId = data.readInt();
        int batchId = data.readInt();
        int size = data.readInt();

        data.resetReaderIndex();
        logger.debug(
                "mappartition filename:{} write partition:{} currentSubpartition:{} attemptId:{} batchId:{} size:{}",
                fileInfo.getFilePath(),
                partitionId,
                currentSubpartition,
                attemptId,
                batchId,
                size);

        regionStartOrFinish(partitionId);

        if (partitionId < currentSubpartition) {
            throw new IOException(
                    "Must writing data in reduce partition index order, but now partitionId is "
                            + partitionId
                            + " and pre partitionId is "
                            + currentSubpartition);
        }

        if (partitionId > currentSubpartition) {
            currentSubpartition = partitionId;
        }
        long length = data.readableBytes();
        totalBytes += length;
        numSubpartitionBytes[partitionId] += length;
        writeDataToFile(data);
        isRegionFinished = false;
        if (!subpartitionHasStartSegment.isEmpty()) {
            if (!subpartitionHasStartSegment.containsKey(partitionId)) {
                throw new IllegalStateException("This partition may not start a segment: " + partitionId);
            }
            if (subpartitionHasStartSegment.get(partitionId)) {
                fileInfo.addSegmentIdAndFirstBufferIndex(
                        partitionId,
                        subpartitionBufferIndex[partitionId],
                        fileInfo.getPartitionWritingSegmentId(partitionId));
                logger.debug(
                        "Add a segment id, partitionId:{}, bufferIndex:{} segmentId: {}, filename:{} attemptId:{}",
                        partitionId,
                        subpartitionBufferIndex[partitionId],
                        fileInfo.getPartitionWritingSegmentId(partitionId),
                        fileInfo.getFilePath(),
                        attemptId);
                subpartitionHasStartSegment.put(partitionId, false);
            }
            subpartitionBufferIndex[partitionId]++;
        }
    }

    @Override
    public void regionStart(int currentDataRegionIndex, boolean isBroadcastRegion) {
        if (hasRegionStarted) {
            return;
        }
        super.regionStart(currentDataRegionIndex, isBroadcastRegion);
        this.hasRegionStarted = true;
    }

    @Override
    public void regionFinish() throws IOException {
        hasRegionStarted = false;
        super.regionFinish();
        currentRegionIndex++;
    }

    public void segmentStart(int partitionId, int segmentId) {
        fileInfo.addPartitionSegmentId(partitionId, segmentId);
        subpartitionHasStartSegment.put(partitionId, true);
    }

    private void regionStartOrFinish(int subpartitionId) throws IOException {
        regionStart(currentRegionIndex, false);
        if (subpartitionId < currentSubpartition) {
            regionFinish();
            logger.info("Check region {} finish subpartition id {} and start next region {}",
                    fileInfo.getFilePath(), subpartitionId, currentRegionIndex);
            regionStart(currentRegionIndex, false);
        }
    }
}
