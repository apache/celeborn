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

import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.read.CelebornInputStream;
import org.apache.celeborn.client.read.MetricsCallback;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.unsafe.Platform;

public class CelebornTezReader {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CelebornTezReader.class);

  private int shuffleId;
  private int partitionId;
  private int attemptNumber;
  private ShuffleClient shuffleClient;
  private long inputShuffleSize;
  private CelebornInputStream celebornInputStream;

  public CelebornTezReader(
      ShuffleClient shuffleClient, int shuffleId, int partitionId, int attemptNumber) {
    this.shuffleClient = shuffleClient;
    this.partitionId = partitionId;
    this.attemptNumber = attemptNumber;
    this.shuffleId = shuffleId;
  }

  public void init() throws IOException {
    MetricsCallback metricsCallback =
        new MetricsCallback() {
          @Override
          public void incBytesRead(long bytesRead) {}

          @Override
          public void incReadTime(long time) {}
        };
    celebornInputStream =
        shuffleClient.readPartition(
            shuffleId, partitionId, attemptNumber, 0, Integer.MAX_VALUE, metricsCallback);
  }

  public byte[] getShuffleBlock() throws IOException {
    // get len
    byte[] header = new byte[4];
    int count = celebornInputStream.read(header);
    if (count == -1) {
      return null;
    }
    while (count != header.length) {
      count += celebornInputStream.read(header, count, 4 - count);
    }

    // get data
    int blockLen = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET);
    inputShuffleSize += blockLen;
    byte[] shuffleData = new byte[blockLen];
    count = celebornInputStream.read(shuffleData);
    while (count != shuffleData.length) {
      count += celebornInputStream.read(shuffleData, count, blockLen - count);
      if (count == -1) {
        // read shuffle is done.
        throw new CelebornIOException("Read mr shuffle failed.");
      }
    }
    return shuffleData;
  }

  public void close() throws IOException {
    celebornInputStream.close();
  }

  public int getPartitionId() {
    return partitionId;
  }
}
