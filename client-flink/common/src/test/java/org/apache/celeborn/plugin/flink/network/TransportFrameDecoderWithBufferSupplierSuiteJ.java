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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.protocol.Message;
import org.apache.celeborn.common.network.protocol.ReadData;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.protocol.SubPartitionReadData;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PbBacklogAnnouncement;
import org.apache.celeborn.common.util.JavaUtils;

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
    RequestMessage unUsedReadData = generateReadDataMessage(1, 0, generateData(1024));
    RequestMessage readData = generateReadDataMessage(2, 0, generateData(1024));
    RpcRequest announcement1 = createBacklogAnnouncement(0, 0);
    RequestMessage unUsedReadData1 = generateReadDataMessage(1, 0, generateData(1024));
    RequestMessage readData1 = generateReadDataMessage(2, 0, generateData(8));

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

  private RequestMessage generateReadDataMessage(long streamId, int subPartitionId, ByteBuf buf) {
    if (testReadDataType == TestReadDataType.READ_DATA) {
      return new ReadData(streamId, buf);
    } else {
      return new SubPartitionReadData(streamId, subPartitionId, buf);
    }
  }
}
