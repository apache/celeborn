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

package com.aliyun.emr.rss.common.network.util;

import java.util.LinkedList;
import java.util.function.Function;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import com.aliyun.emr.rss.common.network.protocol.Message;

public class TransportFrameDecoderWithBufferSupplier extends ChannelInboundHandlerAdapter {
  public static final String HANDLER_NAME = "frameDecoder";
  // Message size + Msg type + Body size
  private static final int HEADER_SIZE = 4 + 1 + 4;
  private final LinkedList<ByteBuf> buffers = new LinkedList<>();
  private final Function<Long, Supplier<ByteBuf>> bufferSuppliers;
  private ByteBuf externalBuf = null;
  private CompositeByteBuf bodyBuf = null;
  private final ByteBuf headerBuf = Unpooled.buffer(HEADER_SIZE, HEADER_SIZE);
  private final ByteBuf msgBuf = Unpooled.buffer(8);
  private Message curMsg = null;
  private Message.Type curType = Message.Type.UnkownType;
  private int msgSize = -1;
  private int bodySize = -1;

  public TransportFrameDecoderWithBufferSupplier() {
    this.bufferSuppliers = null;
  }

  public TransportFrameDecoderWithBufferSupplier(Function<Long, Supplier<ByteBuf>> bufferSuppliers) {
    this.bufferSuppliers = bufferSuppliers;
  }

  private void copyByteBuf(ByteBuf source, ByteBuf target, int targetSize) {
    int bytes = Math.min(source.readableBytes(), targetSize - target.readableBytes());
    target.writeBytes(source, bytes);
  }

  private void decodeHeader(ByteBuf buf, ChannelHandlerContext ctx) {
    copyByteBuf(buf, headerBuf, HEADER_SIZE);
    if (!headerBuf.isWritable()) {
      msgSize = headerBuf.readInt();
      if (msgBuf.capacity() < msgSize) {
        msgBuf.capacity(msgSize);
      }
      msgBuf.clear();
      curType = Message.Type.decode(headerBuf);
      bodySize = headerBuf.readInt();
      decodeMsg(buf, ctx);
    }
  }

  private void decodeMsg(ByteBuf buf, ChannelHandlerContext ctx) {
    if (msgBuf.readableBytes() < msgSize) {
      copyByteBuf(buf, msgBuf, msgSize);
    }
    if (msgBuf.readableBytes() == msgSize) {
      curMsg = Message.decode(curType, msgBuf);
      if (bodySize <= 0) {
        ctx.fireChannelRead(curMsg);
        clear();
      }
    }
  }

  private ByteBuf decodeBody(ByteBuf buf, ChannelHandlerContext ctx) {
    if (bodyBuf == null) {
      if (buf.readableBytes() >= bodySize) {
        ByteBuf body = buf.retain().readSlice(bodySize);
        curMsg.setBody(body);
        ctx.fireChannelRead(curMsg);
        clear();
        return buf;
      } else {
        bodyBuf = buf.alloc().compositeBuffer(Integer.MAX_VALUE);
      }
    }
    int remaining = bodySize - bodyBuf.readableBytes();
    ByteBuf next;
    if (remaining >= buf.readableBytes()) {
      next = buf;
      buf = null;
    } else {
      next = buf.retain().readSlice(remaining);
    }
    bodyBuf.addComponent(next).writerIndex(bodyBuf.writerIndex() + next.readableBytes());
    if (bodyBuf.readableBytes() == bodySize) {
      curMsg.setBody(bodyBuf);
      ctx.fireChannelRead(curMsg);
      clear();
    }
    return buf;
  }

  private ByteBuf decodeBodyCopyOut(ByteBuf buf, ChannelHandlerContext ctx) {
    if (externalBuf == null) {
      // TODO default value
      externalBuf = bufferSuppliers.apply(-1L).get();
    }
    copyByteBuf(buf, externalBuf, bodySize);
    if (externalBuf.readableBytes() == bodySize) {
      curMsg.setBody(externalBuf);
      ctx.fireChannelRead(curMsg);
      clear();
    }
    return buf;
  }

  public void channelRead(ChannelHandlerContext ctx, Object data) {
    ByteBuf buf = (ByteBuf) data;
    try {
      while (buf != null && buf.isReadable()) {
        if (headerBuf.isWritable()) {
          decodeHeader(buf, ctx);
        } else if (curMsg == null) {
          decodeMsg(buf, ctx);
        } else if (bodySize > 0) {
          if (curMsg.needCopyOut()) {
            buf = decodeBodyCopyOut(buf, ctx);
          } else {
            buf = decodeBody(buf, ctx);
          }
        }
      }
    } finally {
      if (buf != null) {
        buf.release();
      }
    }
  }

  private void clear() {
    if (externalBuf != null) {
      externalBuf.clear();
    }
    curMsg = null;
    curType = Message.Type.UnkownType;
    headerBuf.clear();
    bodyBuf = null;
    bodySize = -1;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    // Release all buffers that are still in our ownership.
    // Doing this in handlerRemoved(...) guarantees that this will happen in all cases:
    //     - When the Channel becomes inactive
    //     - When the decoder is removed from the ChannelPipeline
    for (ByteBuf b : buffers) {
      b.release();
    }
    clear();
    buffers.clear();
    headerBuf.release();
    super.handlerRemoved(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    super.exceptionCaught(ctx, cause);
  }
}
