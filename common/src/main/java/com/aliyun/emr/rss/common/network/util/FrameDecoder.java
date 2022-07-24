package com.aliyun.emr.rss.common.network.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public interface FrameDecoder {
  String HANDLER_NAME = "frameDecoder";
  // Message size + Msg type + Body size
  int HEADER_SIZE = 4 + 1 + 4;
}
