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

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.read.CelebornInputStream;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.unsafe.Platform;

public class CelebornShuffleReader<K, V> {
  private static Logger logger = LoggerFactory.getLogger(CelebornShuffleReader.class);
  private TaskAttemptID reduceId;
  private final Reporter reporter;
  private final TaskStatus status;
  private final MergeManager<K, V> merger;
  private final Progress progress;
  private final ShuffleClientMetrics metrics;
  private final CelebornInputStream celebornInputStream;
  private volatile boolean stopped = false;
  private int uniqueMapId = 0;
  private final Counters.Counter ioErrs;
  private boolean hasPendingData = false;
  private long inputShuffleSize;
  private byte[] shuffleData;

  public CelebornShuffleReader(
      TaskAttemptID reduceId,
      TaskStatus status,
      MergeManager<K, V> merger,
      Progress progress,
      Reporter reporter,
      ShuffleClientMetrics metrics,
      CelebornInputStream input) {
    this.reduceId = reduceId;
    this.reporter = reporter;
    this.status = status;
    this.merger = merger;
    this.progress = progress;
    this.metrics = metrics;
    this.celebornInputStream = input;

    ioErrs = reporter.getCounter("Shuffle Errors", "IO_ERROR");
  }

  // fetch all push data and merge
  public void fetchAndMerge() throws InterruptedException, IOException {
    while (!stopped) {
      try {
        // If merge is on, block
        merger.waitForResource();
        // Do shuffle
        metrics.threadBusy();
        // read blocks
        doReadAndMerge();
      } finally {
        metrics.threadFree();
      }
    }
  }

  private byte[] getShuffleBlock() throws IOException {
    // get len
    byte[] header = new byte[4];
    int count = celebornInputStream.read(header);
    if (count == -1) {
      stopped = true;
      return null;
    }
    while (count != header.length) {
      count += celebornInputStream.read(header);
    }

    // get data
    int blockLen = Platform.getInt(header, 0);
    inputShuffleSize += blockLen;
    byte[] shuffleData = new byte[blockLen];
    count = 0;
    count = celebornInputStream.read(header);
    if (count == -1) {
      // read shuffle is done.
      stopped = true;
      throw new CelebornIOException("Read mr shuffle failed.");
    }
    while (count != shuffleData.length) {
      count += celebornInputStream.read(header);
    }
    return shuffleData;
  }

  private void doReadAndMerge() throws IOException {
    if (!hasPendingData) {
      shuffleData = getShuffleBlock();
    }

    if (shuffleData != null) {
      // start to merge
      if (wrapMapOutput(shuffleData)) {
        hasPendingData = false;
      } else {
        return;
      }

      updateStatus();
      reporter.progress();
    } else {
      celebornInputStream.close();
      metrics.inputBytes(inputShuffleSize);
      logger.info("reduce task " + reduceId.toString() + "read" + inputShuffleSize + "bytes");
      stopped = true;
    }
  }

  private boolean wrapMapOutput(byte[] shuffleData) throws IOException {
    // treat push data as mapoutput
    TaskAttemptID mapId =
        new TaskAttemptID(new TaskID(reduceId.getJobID(), TaskType.MAP, uniqueMapId++), 0);
    MapOutput<K, V> mapOutput = null;
    try {
      mapOutput = merger.reserve(mapId, shuffleData.length, 0);
    } catch (IOException ioe) {
      // kill this reduce attempt
      ioErrs.increment(1);
      throw ioe;
    }
    if (mapOutput == null) {
      logger.info("RssMRFetcher" + " - MergeManager returned status WAIT ...");
      hasPendingData = true;
      return false;
    }

    // write data to mapOutput
    try {
      writeShuffle(mapOutput, shuffleData);
      // let the merger knows this block is ready for merging
      mapOutput.commit();
      if (mapOutput instanceof OnDiskMapOutput) {
        logger.info(
            "Reduce: "
                + reduceId
                + " allocates disk to accept block "
                + " with byte sizes: "
                + shuffleData.length);
      }
    } catch (Throwable t) {
      ioErrs.increment(1);
      mapOutput.abort();
      throw new CelebornIOException(
          "Reduce: "
              + reduceId
              + " cannot write block to "
              + mapOutput.getClass().getSimpleName()
              + " due to: "
              + t.getClass().getName());
    }
    return true;
  }

  private void writeShuffle(MapOutput mapOutput, byte[] shuffle) throws CelebornIOException {
    if (mapOutput instanceof InMemoryMapOutput) {
      InMemoryMapOutput inMemoryMapOutput = (InMemoryMapOutput) mapOutput;
      byte[] memory = inMemoryMapOutput.getMemory();
      System.arraycopy(shuffle, 0, memory, 0, shuffle.length);
    } else if (mapOutput instanceof OnDiskMapOutput) {
      throw new IllegalStateException(
          "Celeborn map reduce client do not support OnDiskMapOutput. Try to increase mapreduce.reduce.shuffle.memory.limit.percent");
    } else {
      throw new IllegalStateException(
          "Merger reserve unknown type of MapOutput: " + mapOutput.getClass().getCanonicalName());
    }
  }

  private void updateStatus() {
    progress.set(
        (float) celebornInputStream.readPartitions() / celebornInputStream.totalPartitionsToRead());
    String statusString =
        celebornInputStream.readPartitions()
            + " / "
            + celebornInputStream.totalPartitionsToRead()
            + " copied.";
    status.setStateString(statusString);

    progress.setStatus(
        "copy("
            + celebornInputStream.readPartitions()
            + " of "
            + celebornInputStream.totalPartitionsToRead());
  }
}
