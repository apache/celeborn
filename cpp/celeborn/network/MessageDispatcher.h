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

#include <wangle/bootstrap/ClientBootstrap.h>
#include <wangle/channel/AsyncSocketHandler.h>
#include <wangle/channel/EventBaseHandler.h>
#include <wangle/service/ClientDispatcher.h>

#include "celeborn/conf/CelebornConf.h"
#include "celeborn/network/Message.h"
#include "celeborn/protocol/ControlMessages.h"
#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
namespace network {
using SerializePipeline =
    wangle::Pipeline<folly::IOBufQueue&, std::unique_ptr<Message>>;

/**
 * MessageDispatcher is responsible for:
 *  1. Record the connection between MessageFuture and MessagePromise.
 *     When a request message is sent via write(), the MessageFuture is
 *     recorded; then when the response message is received via read(),
 *     the response would be transferred to MessageFuture by fulfilling
 *     the corresponding MessagePromise.
 *  2. Send different messages via different interfaces, and calls
 *     write() to send it to the network layer. A MessagePromise is
 *     created and recorded for each returned MessageFuture.
 *  3. Receive response messages via read(), and dispatch the message
 *     according to the message kind, and finally fulfills the
 *     corresponding MessagePromise.
 *  4. Handles and reports all kinds of network issues, e.g. EOF,
 *     inactive, exception, etc.
 */
class MessageDispatcher : public wangle::ClientDispatcherBase<
                              SerializePipeline,
                              std::unique_ptr<Message>,
                              std::unique_ptr<Message>> {
 public:
  void read(Context*, std::unique_ptr<Message> toRecvMsg) override;

  virtual folly::Future<std::unique_ptr<Message>> sendRpcRequest(
      std::unique_ptr<Message> toSendMsg) {
    return operator()(std::move(toSendMsg));
  }

  virtual folly::Future<std::unique_ptr<Message>> sendFetchChunkRequest(
      const protocol::StreamChunkSlice& streamChunkSlice,
      std::unique_ptr<Message> toSendMsg);

  virtual void sendRpcRequestWithoutResponse(
      std::unique_ptr<Message> toSendMsg);

  folly::Future<std::unique_ptr<Message>> operator()(
      std::unique_ptr<Message> toSendMsg) override;

  void readEOF(Context* ctx) override;

  void readException(Context* ctx, folly::exception_wrapper e) override;

  void transportActive(Context* ctx) override;

  void transportInactive(Context* ctx) override;

  folly::Future<folly::Unit> writeException(
      Context* ctx,
      folly::exception_wrapper e) override;

  folly::Future<folly::Unit> close() override;

  folly::Future<folly::Unit> close(Context* ctx) override;

  bool isAvailable() override {
    return !closed_;
  }

 private:
  void cleanup();

  using MsgPromise = folly::Promise<std::unique_ptr<Message>>;
  struct MsgPromiseHolder {
    MsgPromise msgPromise;
    std::chrono::time_point<std::chrono::system_clock> requestTime;
  };
  folly::Synchronized<std::unordered_map<long, MsgPromiseHolder>, std::mutex>
      requestIdRegistry_;
  folly::Synchronized<
      std::unordered_map<
          protocol::StreamChunkSlice,
          MsgPromiseHolder,
          protocol::StreamChunkSlice::Hasher>,
      std::mutex>
      streamChunkSliceRegistry_;
  std::atomic<bool> closed_{false};
};
} // namespace network
} // namespace celeborn
