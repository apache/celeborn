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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.service.deploy.worker.storage.MapPartitionData;
import org.apache.celeborn.service.deploy.worker.storage.MapPartitionDataReader;

public class SegmentMapPartitionData extends MapPartitionData {

  public static final Logger logger = LoggerFactory.getLogger(SegmentMapPartitionData.class);

  public SegmentMapPartitionData(
      int minReadBuffers,
      int maxReadBuffers,
      ConcurrentHashMap<String, ExecutorService> storageFetcherPool,
      int threadsPerMountPoint,
      DiskFileInfo fileInfo,
      Consumer<Long> recycleStream,
      int minBuffersToTriggerRead)
      throws IOException {
    super(
        minReadBuffers,
        maxReadBuffers,
        storageFetcherPool,
        threadsPerMountPoint,
        fileInfo,
        recycleStream,
        minBuffersToTriggerRead);
  }

  @Override
  public void setupDataPartitionReader(
      int startSubIndex, int endSubIndex, long streamId, Channel channel) {
    SegmentMapPartitionDataReader mapPartitionDataReader =
        new SegmentMapPartitionDataReader(
            startSubIndex,
            endSubIndex,
            getDiskFileInfo(),
            streamId,
            channel,
            () -> recycleStream.accept(streamId));
    logger.debug(
        "Setup data partition reader from {} to {} with streamId {}",
        startSubIndex,
        endSubIndex,
        streamId);
    readers.put(streamId, mapPartitionDataReader);
  }

  @Override
  protected void openReader(MapPartitionDataReader reader) throws IOException {
    super.openReader(reader);
    if (reader instanceof SegmentMapPartitionDataReader) {
      ((SegmentMapPartitionDataReader) reader).updateSegmentId();
    } else {
      logger.warn("openReader only expects SegmentMapPartitionDataReader.");
    }
  }

  @Override
  public String toString() {
    return String.format("SegmentMapPartitionData{filePath=%s}", diskFileInfo.getFilePath());
  }

  public void notifyRequiredSegmentId(int segmentId, long streamId, int subPartitionId) {
    MapPartitionDataReader streamReader = getStreamReader(streamId);
    if (!(streamReader instanceof SegmentMapPartitionDataReader)) {
      logger.warn("notifyRequiredSegmentId only expects non-null SegmentMapPartitionDataReader.");
      return;
    }
    ((SegmentMapPartitionDataReader) streamReader)
        .notifyRequiredSegmentId(segmentId, subPartitionId);
    // After notifying the required segment id, we need to try to send data again.
    readExecutor.submit(
        () -> {
          try {
            streamReader.sendData();
          } catch (Throwable throwable) {
            logger.error("Failed to send data.", throwable);
          }
        });
  }
}
