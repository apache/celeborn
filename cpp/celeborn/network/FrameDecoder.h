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

#pragma once

#include <folly/io/Cursor.h>
#include <wangle/codec/ByteToMessageDecoder.h>

namespace celeborn {
namespace network {
/**
 * A complete Message encoding/decoding frame is:
 * -----------------------------------------------------------------------
 * | encodedLength | msgType | bodyLength | encodedContent | bodyContent |
 * -----------------------------------------------------------------------
 * The size of each part is:
 * -----------------------------------------------------------------------
 * | 4             | 1       | 4          | #encodedLength | #bodyLength |
 * -----------------------------------------------------------------------
 * So the #headerLength is 4 + 1 + 4,
 * and the complete frameLength is:
 *   #frameLength = #headerLength + #encodedLength + #bodyLength.
 */

class FrameDecoder : public wangle::ByteToByteDecoder {
 public:
  FrameDecoder(bool networkByteOrder = true)
      : networkByteOrder_(networkByteOrder) {}

  bool decode(
      Context* ctx,
      folly::IOBufQueue& buf,
      std::unique_ptr<folly::IOBuf>& result,
      size_t&) override {
    if (buf.chainLength() < headerLength_) {
      return false;
    }

    folly::io::Cursor c(buf.front());
    int encodedLength, bodyLength;
    if (networkByteOrder_) {
      encodedLength = c.readBE<int32_t>();
      c.skip(1);
      bodyLength = c.readBE<int32_t>();
    } else {
      encodedLength = c.readLE<int32_t>();
      c.skip(1);
      bodyLength = c.readLE<int32_t>();
    }

    uint64_t frameLength = headerLength_ + encodedLength + bodyLength;
    if (buf.chainLength() < frameLength) {
      return false;
    }

    result = buf.split(frameLength);
    return true;
  }

 private:
  bool networkByteOrder_;

  static constexpr int lenEncodedLength_ = 4;
  static constexpr int lenMsgType_ = 1;
  static constexpr int lenBodyLength_ = 4;
  static constexpr int headerLength_ =
      lenEncodedLength_ + lenMsgType_ + lenBodyLength_;
};
} // namespace network
} // namespace celeborn
