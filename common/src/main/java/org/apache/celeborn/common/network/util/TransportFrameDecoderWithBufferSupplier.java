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

package org.apache.celeborn.common.network.util;

import java.util.function.Function;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.celeborn.common.network.protocol.Message;

public class TransportFrameDecoderWithBufferSupplier extends ChannelInboundHandlerAdapter
    implements FrameDecoder {
  private final Function<Integer, Supplier<ByteBuf>> bufferSuppliers;
  private int msgSize = -1;
  private int bodySize = -1;
  private Message.Type curType = Message.Type.UNKNOWN_TYPE;
  private ByteBuf headerBuf = Unpooled.buffer(HEADER_SIZE, HEADER_SIZE);
  private CompositeByteBuf bodyBuf = null;
  private ByteBuf externalBuf = null;
  private final ByteBuf msgBuf = Unpooled.buffer(8);
  private Message curMsg = null;

  public TransportFrameDecoderWithBufferSupplier() {
    this.bufferSuppliers =
        new Function<Integer, Supplier<ByteBuf>>() {
          @Override
          public Supplier<ByteBuf> apply(Integer size) {
            return new Supplier<ByteBuf>() {
              @Override
              public ByteBuf get() {
                return Unpooled.buffer(size);
              }
            };
          }
        };
  }

  public TransportFrameDecoderWithBufferSupplier(
      Function<Integer, Supplier<ByteBuf>> bufferSuppliers) {
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
      curMsg = Message.decode(curType, msgBuf, false);
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
      externalBuf = bufferSuppliers.apply(bodySize).get();
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
    externalBuf = null;
    curMsg = null;
    curType = Message.Type.UNKNOWN_TYPE;
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
    clear();
    if (externalBuf != null) {
      externalBuf.clear();
    }
    headerBuf.release();
    super.handlerRemoved(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    super.exceptionCaught(ctx, cause);
  }
}
