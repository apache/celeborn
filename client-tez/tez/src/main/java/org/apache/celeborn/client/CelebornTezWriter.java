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

package org.apache.celeborn.client;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.write.DataPusher;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;

public class CelebornTezWriter {
  private final Logger logger = LoggerFactory.getLogger(CelebornTezWriter.class);

  private final ShuffleClient shuffleClient;
  private DataPusher dataPusher;
  private final int shuffleId;
  private final int mapId;
  private final int attemptNumber;
  private final int numMappers;
  private final int numPartitions;
  private final LongAdder[] mapStatusLengths;

  public CelebornTezWriter(
      int shuffleId,
      int mapId,
      int attemptNumber,
      long taskAttemptId,
      int numMappers,
      int numPartitions,
      CelebornConf conf,
      String appUniqueId,
      String lifecycleManagerHost,
      int lifecycleManagerPort,
      UserIdentifier userIdentifier) {
    this(
        shuffleId,
        mapId,
        attemptNumber,
        taskAttemptId,
        numMappers,
        numPartitions,
        conf,
        ShuffleClient.get(
            appUniqueId, lifecycleManagerHost, lifecycleManagerPort, conf, userIdentifier, null));
  }

  @VisibleForTesting
  CelebornTezWriter(
      int shuffleId,
      int mapId,
      int attemptNumber,
      long taskAttemptId,
      int numMappers,
      int numPartitions,
      CelebornConf conf,
      ShuffleClient shuffleClient) {
    this.shuffleClient = shuffleClient;
    // TEZ_SHUFFLE_ID
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptNumber = attemptNumber;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;

    mapStatusLengths = new LongAdder[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      mapStatusLengths[i] = new LongAdder();
    }
    try {
      dataPusher =
          new DataPusher(
              shuffleId,
              mapId,
              attemptNumber,
              taskAttemptId,
              numMappers,
              numPartitions,
              conf,
              shuffleClient,
              null,
              integer -> {},
              mapStatusLengths);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  CelebornTezWriter(
      int shuffleId,
      int mapId,
      int attemptNumber,
      int numMappers,
      int numPartitions,
      ShuffleClient shuffleClient,
      DataPusher dataPusher) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptNumber = attemptNumber;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.shuffleClient = shuffleClient;
    this.dataPusher = dataPusher;
    mapStatusLengths = new LongAdder[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      mapStatusLengths[i] = new LongAdder();
    }
  }

  public void pushData(int partitionId, byte[] dataBuf, int size) throws IOException {
    try {
      dataPusher.addTask(partitionId, dataBuf, size);
    } catch (InterruptedException e) {
      throw new IOInterruptedException(e);
    }
  }

  public void mergeData(int partitionId, byte[] dataBuf, int size) throws IOException {
    int bytesWritten =
        shuffleClient.mergeData(
            shuffleId,
            mapId,
            attemptNumber,
            partitionId,
            dataBuf,
            0,
            size,
            numMappers,
            numPartitions);
    mapStatusLengths[partitionId].add(bytesWritten);
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public long[] getPartitionStats() {
    // Return a stable snapshot because the underlying counters are updated by the push thread.
    return snapshotPartitionStats(mapStatusLengths);
  }

  static long[] snapshotPartitionStats(LongAdder[] mapStatusLengths) {
    long[] partitionStats = new long[mapStatusLengths.length];
    for (int i = 0; i < mapStatusLengths.length; i++) {
      partitionStats[i] = mapStatusLengths[i].sum();
    }
    return partitionStats;
  }

  public void close() throws IOException {
    logger.info(
        "Call mapper end shuffleId:{} mapId:{} attemptId:{} numMappers:{}",
        0,
        mapId,
        attemptNumber,
        numMappers);
    try {
      dataPusher.waitOnTermination();
      shuffleClient.pushMergedData(shuffleId, mapId, attemptNumber);
      shuffleClient.mapperEnd(shuffleId, mapId, attemptNumber, numMappers, numPartitions);
    } catch (InterruptedException e) {
      throw new IOInterruptedException(e);
    }
  }
}
