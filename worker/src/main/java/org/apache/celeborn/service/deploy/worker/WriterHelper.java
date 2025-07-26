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

package org.apache.celeborn.service.deploy.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.WorkerPartitionLocationInfo;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PartitionSplitMode;
import org.apache.celeborn.common.protocol.PartitionType;
import org.apache.celeborn.service.deploy.worker.storage.PartitionDataWriter;
import org.apache.celeborn.service.deploy.worker.storage.StorageManager;

public class WriterHelper {

  private static final Logger LOG = LoggerFactory.getLogger(WriterHelper.class);

  private final StorageManager storageManager;
  private final WorkerPartitionLocationInfo partitionLocationInfo;

  public WriterHelper(
      StorageManager storageManager, WorkerPartitionLocationInfo partitionLocationInfo) {
    this.storageManager = storageManager;
    this.partitionLocationInfo = partitionLocationInfo;
  }

  public List<PartitionLocation> createWriters(
      String shuffleKey,
      String applicationId,
      int shuffleId,
      List<PartitionLocation> requestLocs,
      long splitThreshold,
      PartitionSplitMode splitMode,
      PartitionType partitionType,
      boolean rangeReadFilter,
      UserIdentifier userIdentifier,
      boolean partitionSplitEnabled,
      boolean isSegmentGranularityVisible,
      boolean isPrimary) {
    List<PartitionLocation> locs = new ArrayList<>();
    try {
      ConcurrentLinkedQueue<PartitionLocation> locQueue = new ConcurrentLinkedQueue<>();
      requestLocs
          .parallelStream()
          .forEach(
              requestLoc -> {
                PartitionLocation location =
                    isPrimary
                        ? partitionLocationInfo.getPrimaryLocation(
                            shuffleKey, requestLoc.getUniqueId())
                        : partitionLocationInfo.getReplicaLocation(
                            shuffleKey, requestLoc.getUniqueId());
                if (location == null) {
                  location = requestLoc;
                  PartitionDataWriter writer;
                  try {
                    writer =
                        storageManager.createPartitionDataWriter(
                            applicationId,
                            shuffleId,
                            location,
                            splitThreshold,
                            splitMode,
                            partitionType,
                            rangeReadFilter,
                            userIdentifier,
                            partitionSplitEnabled,
                            isSegmentGranularityVisible);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  locQueue.add(new WorkingPartition(location, writer));
                } else {
                  locQueue.add(location);
                }
              });
      locs.addAll(locQueue);
    } catch (Exception e) {
      LOG.error("CreateWriter for {} failed.", shuffleKey, e);
    }
    return locs;
  }

  public void destroyWriters(List<PartitionLocation> locs, String shuffleKey) {
    locs.parallelStream()
        .forEach(
            partitionLocation -> {
              WorkingPartition workingPartition = (WorkingPartition) partitionLocation;
              PartitionDataWriter fileWriter = workingPartition.getFileWriter();
              fileWriter.destroy(
                  new IOException(
                      String.format(
                          "Destroy FileWriter %s caused by " + "reserving slots failed for %s.",
                          fileWriter, shuffleKey)));
            });
  }
}
