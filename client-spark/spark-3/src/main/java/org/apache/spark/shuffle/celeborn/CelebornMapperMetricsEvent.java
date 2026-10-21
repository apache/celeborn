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

package org.apache.spark.shuffle.celeborn;

import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * Spark listener event carrying per-mapper metrics reported through MapperEnd, aggregated by the
 * Celeborn UI extension. Persisted to the event log and replayed by the History Server: keep the
 * no-arg constructor and bean getters/setters for Jackson, and add new metrics as fields so old
 * event logs and old readers stay compatible.
 */
public class CelebornMapperMetricsEvent implements SparkListenerEvent {

  private int shuffleId;

  // Bytes pushed before compression/encryption; 0 means not reported.
  private long uncompressedBytesWritten;

  // Bytes pushed after compression/encryption; 0 means not reported.
  private long compressedBytesWritten;

  public CelebornMapperMetricsEvent() {}

  public CelebornMapperMetricsEvent(
      int shuffleId, long uncompressedBytesWritten, long compressedBytesWritten) {
    this.shuffleId = shuffleId;
    this.uncompressedBytesWritten = uncompressedBytesWritten;
    this.compressedBytesWritten = compressedBytesWritten;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public void setShuffleId(int shuffleId) {
    this.shuffleId = shuffleId;
  }

  public long getUncompressedBytesWritten() {
    return uncompressedBytesWritten;
  }

  public void setUncompressedBytesWritten(long uncompressedBytesWritten) {
    this.uncompressedBytesWritten = uncompressedBytesWritten;
  }

  public long getCompressedBytesWritten() {
    return compressedBytesWritten;
  }

  public void setCompressedBytesWritten(long compressedBytesWritten) {
    this.compressedBytesWritten = compressedBytesWritten;
  }

  @Override
  public boolean logEvent() {
    return true;
  }
}
