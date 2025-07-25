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
    shuffleClient =
        ShuffleClient.get(
            appUniqueId, lifecycleManagerHost, lifecycleManagerPort, conf, userIdentifier, null);
    // TEZ_SHUFFLE_ID
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptNumber = attemptNumber;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;

    LongAdder[] mapStatusLengths = new LongAdder[numPartitions];
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
  }

  public int getNumPartitions() {
    return numPartitions;
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
