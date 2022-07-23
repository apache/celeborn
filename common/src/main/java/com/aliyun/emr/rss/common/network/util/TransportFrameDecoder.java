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

import com.aliyun.emr.rss.common.network.protocol.Message;
import com.aliyun.emr.rss.common.network.protocol.MessageEncoder;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A customized frame decoder that allows intercepting raw data.
 * <p>
 * This behaves like Netty's frame decoder (with hard coded parameters that match this library's
 * needs), except it allows an interceptor to be installed to read data directly before it's
 * framed.
 * <p>
 * Unlike Netty's frame decoder, each frame is dispatched to child handlers as soon as it's
 * decoded, instead of building as many frames as the current buffer allows and dispatching
 * all of them. This allows a child handler to install an interceptor if needed.
 * <p>
 * If an interceptor is installed, framing stops, and data is instead fed directly to the
 * interceptor. When the interceptor indicates that it doesn't need to read any more data,
 * framing resumes. Interceptors should not hold references to the data buffers provided
 * to their handle() method.
 */
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {
  private static final Logger logger = LoggerFactory.getLogger(TransportFrameDecoder.class);
  public static final String HANDLER_NAME = "frameDecoder";
  // Message size + Msg type + Body sizb
  private static final int HEADER_SIZE = 4 + 1 + 4;
  private static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;
  private static final int UNKNOWN_FRAME_SIZE = -1;

  private final LinkedList<ByteBuf> buffers = new LinkedList<>();
  private final Function<Long, Supplier<ByteBuf>> bufferSuppliers;
  private ByteBuf externalBuf = null;
  private CompositeByteBuf compositeByteBuf = null;
  private final ByteBuf headerBuf = Unpooled.buffer(HEADER_SIZE, HEADER_SIZE);
  private final ByteBuf msgBuf = Unpooled.buffer(8);
  private Message curMsg = null;
  private Message.Type curType = Message.Type.UnkownType;
  private int msgSize = -1;
  private int bodySize = -1;

  private long totalSize = 0;
  private long nextFrameSize = UNKNOWN_FRAME_SIZE;

  public TransportFrameDecoder() {
    this.bufferSuppliers = null;
  }

  public TransportFrameDecoder(Function<Long, Supplier<ByteBuf>> bufferSuppliers) {
    this.bufferSuppliers = bufferSuppliers;
  }

  private void copyByteBuf(ByteBuf source, ByteBuf target, int targetSize) {
    int bytes = Math.min(source.readableBytes(), targetSize - target.readableBytes());
    target.writeBytes(source, bytes);
  }

  public void channelRead(ChannelHandlerContext ctx, Object data) throws Exception {
    ByteBuf buf = (ByteBuf) data;
    try {
      while (buf.isReadable()) {
        if (headerBuf.isWritable()) {
          copyByteBuf(buf, headerBuf, HEADER_SIZE);
          if (!headerBuf.isWritable()) {
            msgSize = headerBuf.readInt();
            if (msgBuf.capacity() < msgSize) {
              msgBuf.capacity(msgSize);
            }
            msgBuf.clear();
            curType = Message.Type.decode(headerBuf);
            bodySize = headerBuf.readInt();
          }
        } else if (curMsg == null) {
          if (msgBuf.readableBytes() < msgSize) {
            copyByteBuf(buf, msgBuf, msgSize);
          }
          if (msgBuf.readableBytes() == msgSize) {
            curMsg = Message.decode(curType, msgBuf);
          }
        } else if (curMsg.hasBody()) {
          if (curMsg.needCopyOut()) {
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
          } else {
            if (compositeByteBuf == null) {
              compositeByteBuf = buf.alloc().compositeBuffer(Integer.MAX_VALUE);
            }
            int remaining = bodySize - compositeByteBuf.readableBytes();
            ByteBuf next;
            if (remaining > buf.readableBytes()) {
              next = buf.retain();
            } else {
              next = buf.retain().readSlice(remaining);
            }
            compositeByteBuf.addComponent(next).writerIndex(compositeByteBuf.writerIndex() + next.readableBytes());
            if (compositeByteBuf.readableBytes() == bodySize) {
              curMsg.setBody(compositeByteBuf);
              ctx.fireChannelRead(curMsg);
              clear();
            }
          }
        } else {
          ctx.fireChannelRead(curMsg);
          clear();
        }
      }
    } finally {
      buf.release();
    }
  }

  private void clear() {
    externalBuf = null;
    curMsg = null;
    curType = Message.Type.UnkownType;
    headerBuf.clear();
    compositeByteBuf.removeComponents(0, compositeByteBuf.numComponents());
    compositeByteBuf.clear();
  }

  public void channelRead1(ChannelHandlerContext ctx, Object data) throws Exception {
    ByteBuf in = (ByteBuf) data;
    buffers.add(in);
    totalSize += in.readableBytes();

    while (!buffers.isEmpty()) {
      ByteBuf frame = decodeNext();
      if (frame == null) {
        break;
      }
      ctx.fireChannelRead(frame);
    }
  }

  private long decodeFrameSize() {
    if (nextFrameSize != UNKNOWN_FRAME_SIZE || totalSize < HEADER_SIZE) {
      return nextFrameSize;
    }

    // We know there's enough data. If the first buffer contains all the data, great. Otherwise,
    // hold the bytes for the frame length in a composite buffer until we have enough data to read
    // the frame size. Normally, it should be rare to need more than one buffer to read the frame
    // size.
    ByteBuf first = buffers.getFirst();
    if (first.readableBytes() >= HEADER_SIZE) {
      nextFrameSize = first.readLong() - HEADER_SIZE;
      totalSize -= HEADER_SIZE;
      if (!first.isReadable()) {
        buffers.removeFirst().release();
      }
      return nextFrameSize;
    }

    while (headerBuf.readableBytes() < HEADER_SIZE) {
      ByteBuf next = buffers.getFirst();
      int toRead = Math.min(next.readableBytes(), HEADER_SIZE - headerBuf.readableBytes());
      headerBuf.writeBytes(next, toRead);
      if (!next.isReadable()) {
        buffers.removeFirst().release();
      }
    }

    nextFrameSize = headerBuf.readLong() - HEADER_SIZE;
    totalSize -= HEADER_SIZE;
    headerBuf.clear();
    return nextFrameSize;
  }

  private ByteBuf decodeNext() {
    long frameSize = decodeFrameSize();
    if (frameSize == UNKNOWN_FRAME_SIZE || totalSize < frameSize) {
      return null;
    }

    // Reset size for next frame.
    nextFrameSize = UNKNOWN_FRAME_SIZE;

    Preconditions.checkArgument(frameSize < MAX_FRAME_SIZE, "Too large frame: %s", frameSize);
    Preconditions.checkArgument(frameSize > 0, "Frame length should be positive: %s", frameSize);

    // If the first buffer holds the entire frame, return it.
    int remaining = (int) frameSize;
    if (buffers.getFirst().readableBytes() >= remaining) {
      return nextBufferForFrame(remaining);
    }

    // Otherwise, create a composite buffer.
    CompositeByteBuf frame = buffers.getFirst().alloc().compositeBuffer(Integer.MAX_VALUE);
    while (remaining > 0) {
      ByteBuf next = nextBufferForFrame(remaining);
      remaining -= next.readableBytes();
      frame.addComponent(next).writerIndex(frame.writerIndex() + next.readableBytes());
    }
    assert remaining == 0;
    return frame;
  }

  /**
   * Takes the first buffer in the internal list, and either adjust it to fit in the frame
   * (by taking a slice out of it) or remove it from the internal list.
   */
  private ByteBuf nextBufferForFrame(int bytesToRead) {
    ByteBuf buf = buffers.getFirst();
    ByteBuf frame;

    if (buf.readableBytes() > bytesToRead) {
      frame = buf.retain().readSlice(bytesToRead);
      totalSize -= bytesToRead;
    } else {
      frame = buf;
      buffers.removeFirst();
      totalSize -= frame.readableBytes();
    }

    return frame;
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
