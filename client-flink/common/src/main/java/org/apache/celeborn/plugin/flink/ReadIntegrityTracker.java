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
package org.apache.celeborn.plugin.flink;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CommitMetadata;
import org.apache.celeborn.plugin.flink.client.FlinkShuffleClientImpl;
import org.apache.celeborn.plugin.flink.utils.BufferUtils;

/**
 * Accumulates the read-side {@link CommitMetadata} (CRC32 + byte count) over a consumed
 * subpartition range and reports it to the driver at stream end. Each read path strips its own
 * framing via {@code accumulate*Buffer} so the hashed bytes match what the writer hashed (the body
 * after the batch header).
 */
public class ReadIntegrityTracker {
  private static final Logger LOG = LoggerFactory.getLogger(ReadIntegrityTracker.class);

  private final FlinkShuffleClientImpl client;
  private final int shuffleId;
  private final int partitionId;
  private final int subPartitionIndexStart;
  private final int subPartitionIndexEnd;
  private final CommitMetadata readCommitMetadata = new CommitMetadata();
  private volatile boolean enabled;
  // True once report() has run; the report is terminal whether it succeeded or failed.
  private volatile boolean reportAttempted = false;

  public ReadIntegrityTracker(
      FlinkShuffleClientImpl client,
      int shuffleId,
      int partitionId,
      int subPartitionIndexStart,
      int subPartitionIndexEnd,
      boolean enabled) {
    this.client = client;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.subPartitionIndexStart = subPartitionIndexStart;
    this.subPartitionIndexEnd = subPartitionIndexEnd;
    this.enabled = enabled;
  }

  public boolean isEnabled() {
    return enabled;
  }

  /** Permanently disables tracking for this stream so nothing is reported at stream end. */
  void disable() {
    enabled = false;
  }

  /** Strips the {@code headerPrefix}-byte batch header; a too-short buffer disables tracking. */
  private void accumulatePlainBuffer(ByteBuf flinkBuffer, int headerPrefix) {
    if (flinkBuffer.readableBytes() < headerPrefix) {
      disableForUnexpectedFraming(
          String.format("a buffer shorter than the %d-byte batch header", headerPrefix));
      return;
    }
    readCommitMetadata.addData(
        flinkBuffer.nioBuffer(
            flinkBuffer.readerIndex() + headerPrefix, flinkBuffer.readableBytes() - headerPrefix));
  }

  /**
   * Regular path: strips the {@link BufferUtils#HEADER_LENGTH_PREFIX}-byte header. This path never
   * splits buffers, so a composite disables tracking.
   */
  public void accumulateRegularBuffer(ByteBuf flinkBuffer) {
    if (!enabled) {
      return;
    }
    if (flinkBuffer instanceof CompositeByteBuf) {
      disableForUnexpectedFraming("an unexpected composite buffer on the regular read path");
      return;
    }
    accumulatePlainBuffer(flinkBuffer, BufferUtils.HEADER_LENGTH_PREFIX);
  }

  /**
   * Tiered path: strips the {@code headerPrefix}-byte header. Accepts a plain buffer or a
   * two-component composite (header + data); other shapes disable tracking.
   */
  public void accumulateTieredBuffer(ByteBuf flinkBuffer, int headerPrefix) {
    if (!enabled) {
      return;
    }
    if (flinkBuffer instanceof CompositeByteBuf) {
      CompositeByteBuf composite = (CompositeByteBuf) flinkBuffer;
      if (composite.numComponents() != 2) {
        disableForUnexpectedFraming(
            String.format("a composite buffer with %d components", composite.numComponents()));
        return;
      }
      ByteBuf header = composite.component(0);
      ByteBuf data = composite.component(1);
      if (header.readableBytes() < headerPrefix) {
        disableForUnexpectedFraming(
            String.format(
                "a header component shorter than the %d-byte batch header", headerPrefix));
        return;
      }
      readCommitMetadata.addData(
          header.nioBuffer(
              header.readerIndex() + headerPrefix, header.readableBytes() - headerPrefix),
          data.nioBuffer(data.readerIndex(), data.readableBytes()));
    } else {
      accumulatePlainBuffer(flinkBuffer, headerPrefix);
    }
  }

  /**
   * Reports the accumulated metadata to the driver once, at the last partition's stream end. A
   * failed report is logged and passed to {@code onFailure} — terminal, not retried, since that
   * already fails the channel.
   */
  public void report(Consumer<IOException> onFailure) {
    if (!enabled || reportAttempted) {
      return;
    }
    reportAttempted = true;
    try {
      client.readReducerPartitionEnd(
          shuffleId,
          partitionId,
          subPartitionIndexStart,
          subPartitionIndexEnd,
          readCommitMetadata.getChecksum(),
          readCommitMetadata.getBytes());
    } catch (IOException e) {
      LOG.error(
          "Integrity check report failed for shuffle {} partition {} subpartitions [{}, {}].",
          shuffleId,
          partitionId,
          subPartitionIndexStart,
          subPartitionIndexEnd,
          e);
      onFailure.accept(e);
    }
  }

  private void disableForUnexpectedFraming(String reason) {
    disable();
    LOG.warn(
        "Integrity check skipped for shuffle {} partition {} subpartitions [{}, {}] because the "
            + "reader saw {}.",
        shuffleId,
        partitionId,
        subPartitionIndexStart,
        subPartitionIndexEnd,
        reason);
  }
}
