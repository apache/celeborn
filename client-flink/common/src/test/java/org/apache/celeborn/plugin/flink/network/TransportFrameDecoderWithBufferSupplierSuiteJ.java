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

package org.apache.celeborn.plugin.flink.network;

import static org.apache.celeborn.common.network.client.TransportClient.requestId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.protocol.Message;
import org.apache.celeborn.common.network.protocol.ReadData;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.protocol.SubPartitionReadData;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PbBacklogAnnouncement;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.plugin.flink.utils.BufferUtils;

@RunWith(Parameterized.class)
public class TransportFrameDecoderWithBufferSupplierSuiteJ {

  enum TestReadDataType {
    READ_DATA,
    SUBPARTITION_READ_DATA,
  }

  private TestReadDataType testReadDataType;

  public TransportFrameDecoderWithBufferSupplierSuiteJ(TestReadDataType testReadDataType) {
    this.testReadDataType = testReadDataType;
  }

  @Parameterized.Parameters
  public static Collection prepareData() {
    Object[][] object = {{TestReadDataType.READ_DATA}, {TestReadDataType.SUBPARTITION_READ_DATA}};
    return Arrays.asList(object);
  }

  @Test
  public void testDropUnusedBytes() throws IOException {
    ConcurrentHashMap<Long, Supplier<org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf>>
        supplier = JavaUtils.newConcurrentHashMap();
    List<org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf> buffers = new ArrayList<>();

    supplier.put(
        2L,
        () -> {
          org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf buffer =
              org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.buffer(32000);
          buffers.add(buffer);
          return buffer;
        });

    TransportFrameDecoderWithBufferSupplier decoder =
        new TransportFrameDecoderWithBufferSupplier(supplier);
    ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);

    RpcRequest announcement = createBacklogAnnouncement(0, 0);
    ReadData unUsedReadData = generateReadDataMessage(1, 0, generateData(1024));
    ReadData readData = generateReadDataMessage(2, 0, generateData(1024));
    RpcRequest announcement1 = createBacklogAnnouncement(0, 0);
    ReadData unUsedReadData1 = generateReadDataMessage(1, 0, generateData(1024));
    ReadData readData1 = generateReadDataMessage(2, 0, generateData(8));

    ByteBuf buffer = Unpooled.buffer(5000);
    encodeMessage(announcement, buffer);
    encodeMessage(unUsedReadData, buffer);
    encodeMessage(readData, buffer);
    encodeMessage(announcement1, buffer);
    encodeMessage(unUsedReadData1, buffer);
    encodeMessage(readData1, buffer);

    // simulate
    buffer.retain();
    decoder.channelRead(context, buffer);
    Assert.assertEquals(buffers.get(0).nioBuffer(), readData.body().nioByteBuffer());
    Assert.assertEquals(buffers.get(1).nioBuffer(), readData1.body().nioByteBuffer());

    // simulate 1 - split the unUsedReadData buffer
    buffer.retain();
    buffer.resetReaderIndex();
    decoder.channelRead(context, buffer.retainedSlice(0, 555));
    ByteBuf byteBuf = buffer.retainedSlice(0, buffer.readableBytes());
    byteBuf.readerIndex(555);
    decoder.channelRead(context, byteBuf);

    Assert.assertEquals(buffers.get(2).nioBuffer(), readData.body().nioByteBuffer());
    Assert.assertEquals(buffers.get(3).nioBuffer(), readData1.body().nioByteBuffer());

    // simulate 2 - split the readData buffer
    buffer.retain();
    buffer.resetReaderIndex();
    decoder.channelRead(context, buffer.retainedSlice(0, 1500));
    byteBuf = buffer.retainedSlice(0, buffer.readableBytes());
    byteBuf.readerIndex(1500);
    decoder.channelRead(context, byteBuf);

    Assert.assertEquals(buffers.get(4).nioBuffer(), readData.body().nioByteBuffer());
    Assert.assertEquals(buffers.get(5).nioBuffer(), readData1.body().nioByteBuffer());
    Assert.assertEquals(buffers.size(), 6);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testFailProcessFullBufferIfDisableLargeBufferSplit() throws IOException {
    int bufferSizeBytes = 10 * 1024;
    ConcurrentHashMap<Long, Supplier<org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf>>
        supplier = JavaUtils.newConcurrentHashMap();

    supplier.put(
        0L,
        () ->
            org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.buffer(
                bufferSizeBytes, bufferSizeBytes));

    TransportFrameDecoderWithBufferSupplier decoder =
        new TransportFrameDecoderWithBufferSupplier(
            supplier, TransportFrameDecoderWithBufferSupplier.DISABLE_LARGE_BUFFER_SPLIT_SIZE);
    ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);

    SubPartitionReadData readData =
        new SubPartitionReadData(0, 0, generateData(bufferSizeBytes + BufferUtils.HEADER_LENGTH));

    ByteBuf buffer = Unpooled.buffer(bufferSizeBytes * 4);
    encodeMessage(readData, buffer);

    // simulate
    buffer.retain();
    decoder.channelRead(context, buffer);
  }

  @Test
  public void testProcessFullBufferIfEnableLargeBufferSplit() throws IOException {
    int bufferSizeBytes = 10 * 1024;
    ConcurrentHashMap<Long, Supplier<org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf>>
        supplier = JavaUtils.newConcurrentHashMap();
    List<Message> parsedMessages = new ArrayList<>();

    supplier.put(
        0L,
        () ->
            org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.buffer(
                bufferSizeBytes, bufferSizeBytes));

    TransportFrameDecoderWithBufferSupplier decoder =
        new TransportFrameDecoderWithBufferSupplier(supplier, bufferSizeBytes);
    ChannelHandlerContext context = Mockito.mock(ChannelHandlerContext.class);
    when(context.fireChannelRead(any()))
        .thenAnswer(
            m -> {
              Assert.assertEquals(1, m.getArguments().length);
              parsedMessages.add(m.getArgument(0));
              return null;
            });

    ReadData readData1 = new ReadData(0, generateData(1024));
    // simulate the client received a large buffer which body size large than size of given buffer
    // in this case, the flinkBuffer of parsed message will contain two parts: celeborn header and
    // data buffer
    SubPartitionReadData readData2 =
        new SubPartitionReadData(0, 0, generateData(BufferUtils.HEADER_LENGTH + bufferSizeBytes));
    SubPartitionReadData readData3 = new SubPartitionReadData(0, 0, generateData(1024));

    ByteBuf buffer = Unpooled.buffer(bufferSizeBytes * 4);
    encodeMessage(readData1, buffer);
    encodeMessage(readData2, buffer);
    encodeMessage(readData3, buffer);

    // simulate
    buffer.retain();
    decoder.channelRead(context, buffer);
    Assert.assertEquals(parsedMessages.size(), 3);

    // the parsed first message contains the readData1
    Assert.assertTrue(
        parsedMessages.get(0) instanceof org.apache.celeborn.plugin.flink.protocol.ReadData);
    Assert.assertEquals(
        ((org.apache.celeborn.plugin.flink.protocol.ReadData) parsedMessages.get(0))
            .getFlinkBuffer()
            .nioBuffer(),
        readData1.body().nioByteBuffer());

    // the parsed second message contains the readData2
    Assert.assertTrue(
        parsedMessages.get(1)
            instanceof org.apache.celeborn.plugin.flink.protocol.SubPartitionReadData);
    org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf byteBuf2 =
        ((org.apache.celeborn.plugin.flink.protocol.SubPartitionReadData) parsedMessages.get(1))
            .getFlinkBuffer();
    // verify the flinkBuffer of parsed message contains two parts: celeborn header and data buffer
    Assert.assertTrue(
        byteBuf2 instanceof org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf);
    CompositeByteBuf compositeByteBuf2 = (CompositeByteBuf) byteBuf2;
    Assert.assertEquals(compositeByteBuf2.numComponents(), 2);
    org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf inputByteBuf2 =
        org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled.wrappedBuffer(
            readData2.body().nioByteBuffer());
    // the first part is celeborn header
    Assert.assertEquals(
        compositeByteBuf2.component(0).nioBuffer(),
        inputByteBuf2.slice(0, BufferUtils.HEADER_LENGTH).nioBuffer());
    // the second part is data buffer
    Assert.assertEquals(
        compositeByteBuf2.component(1).nioBuffer(),
        inputByteBuf2.slice(BufferUtils.HEADER_LENGTH, bufferSizeBytes).nioBuffer());

    // the parsed third message contains the readData3
    Assert.assertTrue(
        parsedMessages.get(2)
            instanceof org.apache.celeborn.plugin.flink.protocol.SubPartitionReadData);
    Assert.assertEquals(
        ((org.apache.celeborn.plugin.flink.protocol.SubPartitionReadData) parsedMessages.get(2))
            .getFlinkBuffer()
            .nioBuffer(),
        readData3.body().nioByteBuffer());
  }

  public RpcRequest createBacklogAnnouncement(long streamId, int backlog) {
    return new RpcRequest(
        requestId(),
        new NioManagedBuffer(
            new TransportMessage(
                    MessageType.BACKLOG_ANNOUNCEMENT,
                    PbBacklogAnnouncement.newBuilder()
                        .setStreamId(streamId)
                        .setBacklog(backlog)
                        .build()
                        .toByteArray())
                .toByteBuffer()));
  }

  public ByteBuf encodeMessage(Message in, ByteBuf byteBuf) throws IOException {
    byteBuf.writeInt(in.encodedLength());
    in.type().encode(byteBuf);
    if (in.body() != null) {
      byteBuf.writeInt((int) in.body().size());
      in.encode(byteBuf);
      byteBuf.writeBytes(in.body().nioByteBuffer());
    } else {
      byteBuf.writeInt(0);
      in.encode(byteBuf);
    }

    return byteBuf;
  }

  public ByteBuf generateData(int size) {
    ByteBuf data = Unpooled.buffer(size);
    for (int i = 0; i < size; i++) {
      data.writeByte(new Random().nextInt(7));
    }

    return data;
  }

  private ReadData generateReadDataMessage(long streamId, int subPartitionId, ByteBuf buf) {
    if (testReadDataType == TestReadDataType.READ_DATA) {
      return new ReadData(streamId, buf);
    } else {
      return new SubPartitionReadData(streamId, subPartitionId, buf);
    }
  }
}
