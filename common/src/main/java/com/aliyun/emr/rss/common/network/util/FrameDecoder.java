package com.aliyun.emr.rss.common.network.util;

public interface FrameDecoder {
  String HANDLER_NAME = "frameDecoder";
  // Message size + Msg type + Body size
  int HEADER_SIZE = 4 + 1 + 4;
}
