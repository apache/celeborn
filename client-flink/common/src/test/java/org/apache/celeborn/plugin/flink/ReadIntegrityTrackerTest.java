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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.junit.Test;

import org.apache.celeborn.common.CommitMetadata;
import org.apache.celeborn.plugin.flink.client.FlinkShuffleClientImpl;
import org.apache.celeborn.plugin.flink.utils.BufferUtils;

public class ReadIntegrityTrackerTest {

  private static final int SHUFFLE_ID = 7;
  private static final int PARTITION_ID = 3;
  private static final int START_SUB_INDEX = 2;
  private static final int END_SUB_INDEX = 5;
  private static final int PREFIX = BufferUtils.HEADER_LENGTH_PREFIX;
  private static final Consumer<IOException> NO_OP = e -> {};

  private FlinkShuffleClientImpl mockClient() {
    return mock(FlinkShuffleClientImpl.class);
  }

  private ReadIntegrityTracker newTracker(FlinkShuffleClientImpl client, boolean enabled) {
    return new ReadIntegrityTracker(
        client, SHUFFLE_ID, PARTITION_ID, START_SUB_INDEX, END_SUB_INDEX, enabled);
  }

  /** Deterministic, distinct bytes so different ranges produce different checksums. */
  private static byte[] bytesOfLength(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (i + 1);
    }
    return bytes;
  }

  /** Prepends a distinct {@link #PREFIX}-byte batch header to {@code payload}. */
  private static byte[] frameWithPrefix(byte[] payload) {
    byte[] framed = new byte[PREFIX + payload.length];
    for (int i = 0; i < PREFIX; i++) {
      framed[i] = (byte) (0x80 + i);
    }
    System.arraycopy(payload, 0, framed, PREFIX, payload.length);
    return framed;
  }

  private void verifyReported(FlinkShuffleClientImpl client, CommitMetadata expected)
      throws IOException {
    verify(client)
        .readReducerPartitionEnd(
            SHUFFLE_ID,
            PARTITION_ID,
            START_SUB_INDEX,
            END_SUB_INDEX,
            expected.getChecksum(),
            expected.getBytes());
  }

  @Test
  public void reportSendsAccumulatedMetadataOnce() throws IOException {
    FlinkShuffleClientImpl client = mockClient();
    ReadIntegrityTracker tracker = newTracker(client, true);
    byte[] payload = "regular-path-payload".getBytes();
    tracker.accumulateRegularBuffer(Unpooled.wrappedBuffer(frameWithPrefix(payload)));

    tracker.report(NO_OP);
    tracker.report(NO_OP);

    CommitMetadata expected = new CommitMetadata();
    expected.addData(ByteBuffer.wrap(payload));
    verifyReported(client, expected);
    verify(client, times(1))
        .readReducerPartitionEnd(anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong());
  }

  @Test
  public void reportDoesNothingWhenDisabled() {
    // Disabled at construction (integrity check off).
    FlinkShuffleClientImpl disabledAtStart = mockClient();
    newTracker(disabledAtStart, false).report(NO_OP);
    verifyNoInteractions(disabledAtStart);

    // Disabled mid-stream via disable().
    FlinkShuffleClientImpl disabledLater = mockClient();
    ReadIntegrityTracker tracker = newTracker(disabledLater, true);
    tracker.disable();
    assertFalse(tracker.isEnabled());
    tracker.report(NO_OP);
    verifyNoInteractions(disabledLater);
  }

  @Test
  public void reportDoesNothingAfterAccumulateTriggersDisable() {
    FlinkShuffleClientImpl client = mockClient();
    ReadIntegrityTracker tracker = newTracker(client, true);
    // Accumulate one good buffer, then a malformed one that disables tracking mid-stream.
    tracker.accumulateTieredBuffer(Unpooled.wrappedBuffer(bytesOfLength(PREFIX + 8)), PREFIX);
    assertTrue(tracker.isEnabled());
    tracker.accumulateTieredBuffer(Unpooled.wrappedBuffer(bytesOfLength(PREFIX - 1)), PREFIX);
    assertFalse(tracker.isEnabled());
    // report must not send the partial metadata.
    tracker.report(NO_OP);
    verifyNoInteractions(client);
  }

  @Test
  public void reportPassesFailureToCallbackAndDoesNotRetry() throws IOException {
    FlinkShuffleClientImpl client = mockClient();
    IOException boom = new IOException("report failed");
    doThrow(boom)
        .when(client)
        .readReducerPartitionEnd(anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong());
    ReadIntegrityTracker tracker = newTracker(client, true);

    List<IOException> failures = new ArrayList<>();
    tracker.report(failures::add);
    // A failed report is still terminal: a second call must not re-issue the RPC.
    tracker.report(failures::add);

    assertEquals(1, failures.size());
    assertSame(boom, failures.get(0));
    verify(client, times(1))
        .readReducerPartitionEnd(anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong());
  }

  @Test
  public void accumulateRegularBufferStripsBatchHeaderPrefix() throws IOException {
    FlinkShuffleClientImpl client = mockClient();
    ReadIntegrityTracker tracker = newTracker(client, true);
    // Only the payload after the PREFIX-byte batch header (what the writer hashed) is accumulated.
    byte[] framed = bytesOfLength(PREFIX + 24);
    tracker.accumulateRegularBuffer(Unpooled.wrappedBuffer(framed));
    assertTrue(tracker.isEnabled());
    tracker.report(NO_OP);

    CommitMetadata expected = new CommitMetadata();
    expected.addData(ByteBuffer.wrap(Arrays.copyOfRange(framed, PREFIX, framed.length)));
    verifyReported(client, expected);
  }

  @Test
  public void accumulateRegularBufferDisablesOnUnexpectedFraming() {
    // The regular path never splits large buffers, so any composite buffer is unexpected.
    CompositeByteBuf composite = Unpooled.compositeBuffer();
    composite.addComponent(true, Unpooled.wrappedBuffer(bytesOfLength(32)));
    assertRegularBufferDisables(composite);

    // A plain buffer too short to hold the batch header.
    assertRegularBufferDisables(Unpooled.wrappedBuffer(bytesOfLength(PREFIX - 1)));
  }

  /** Feeding {@code bad} to the regular path must disable tracking and report nothing. */
  private void assertRegularBufferDisables(ByteBuf bad) {
    FlinkShuffleClientImpl client = mockClient();
    ReadIntegrityTracker tracker = newTracker(client, true);
    tracker.accumulateRegularBuffer(bad);
    assertFalse(tracker.isEnabled());
    tracker.report(NO_OP);
    verifyNoInteractions(client);
  }

  @Test
  public void accumulateTieredBufferStripsPrefixFromPlainBuffer() throws IOException {
    FlinkShuffleClientImpl client = mockClient();
    ReadIntegrityTracker tracker = newTracker(client, true);
    byte[] framed = bytesOfLength(PREFIX + 24);
    tracker.accumulateTieredBuffer(Unpooled.wrappedBuffer(framed), PREFIX);
    assertTrue(tracker.isEnabled());
    tracker.report(NO_OP);

    CommitMetadata expected = new CommitMetadata();
    expected.addData(ByteBuffer.wrap(Arrays.copyOfRange(framed, PREFIX, framed.length)));
    verifyReported(client, expected);
  }

  @Test
  public void accumulateTieredBufferHashesHeaderTailAndDataOfComposite() throws IOException {
    FlinkShuffleClientImpl client = mockClient();
    ReadIntegrityTracker tracker = newTracker(client, true);
    // Large buffers arrive as a header component (batch header) plus a data component.
    byte[] header = bytesOfLength(BufferUtils.HEADER_LENGTH);
    byte[] data = "tiered-data-component".getBytes();
    CompositeByteBuf composite = Unpooled.compositeBuffer();
    composite.addComponent(true, Unpooled.wrappedBuffer(header));
    composite.addComponent(true, Unpooled.wrappedBuffer(data));

    tracker.accumulateTieredBuffer(composite, PREFIX);
    assertTrue(tracker.isEnabled());
    tracker.report(NO_OP);

    CommitMetadata expected = new CommitMetadata();
    expected.addData(
        ByteBuffer.wrap(Arrays.copyOfRange(header, PREFIX, header.length)), ByteBuffer.wrap(data));
    verifyReported(client, expected);
  }

  @Test
  public void accumulateTieredBufferDisablesOnUnexpectedFraming() {
    // A composite with a component count other than the expected header + data.
    CompositeByteBuf threeComponents = Unpooled.compositeBuffer();
    threeComponents.addComponent(
        true, Unpooled.wrappedBuffer(bytesOfLength(BufferUtils.HEADER_LENGTH)));
    threeComponents.addComponent(true, Unpooled.wrappedBuffer(bytesOfLength(8)));
    threeComponents.addComponent(true, Unpooled.wrappedBuffer(bytesOfLength(8)));
    assertTieredBufferDisables(threeComponents);

    // A composite whose header component is shorter than the batch-header prefix.
    CompositeByteBuf shortHeader = Unpooled.compositeBuffer();
    shortHeader.addComponent(true, Unpooled.wrappedBuffer(bytesOfLength(PREFIX - 1)));
    shortHeader.addComponent(true, Unpooled.wrappedBuffer(bytesOfLength(8)));
    assertTieredBufferDisables(shortHeader);

    // A plain buffer shorter than the batch-header prefix.
    assertTieredBufferDisables(Unpooled.wrappedBuffer(bytesOfLength(PREFIX - 1)));
  }

  /** Feeding {@code bad} to the tiered path must disable tracking and report nothing. */
  private void assertTieredBufferDisables(ByteBuf bad) {
    FlinkShuffleClientImpl client = mockClient();
    ReadIntegrityTracker tracker = newTracker(client, true);
    tracker.accumulateTieredBuffer(bad, PREFIX);
    assertFalse(tracker.isEnabled());
    tracker.report(NO_OP);
    verifyNoInteractions(client);
  }
}
