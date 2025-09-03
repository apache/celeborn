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

#include <fmt/chrono.h>
#include <folly/io/IOBuf.h>
#include <wangle/bootstrap/ClientBootstrap.h>
#include <wangle/channel/EventBaseHandler.h>
#include <wangle/service/ClientDispatcher.h>

#include "celeborn/conf/CelebornConf.h"
#include "celeborn/network/Message.h"
#include "celeborn/network/MessageDispatcher.h"
#include "celeborn/protocol/ControlMessages.h"
#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
namespace network {
/**
 * MessageSerializeHandler serializes Message into folly::IOBuf when write(),
 * and deserializes folly::IOBuf into Message when read().
 */
class MessageSerializeHandler : public wangle::Handler<
                                    std::unique_ptr<folly::IOBuf>,
                                    std::unique_ptr<Message>,
                                    std::unique_ptr<Message>,
                                    std::unique_ptr<folly::IOBuf>> {
 public:
  void read(Context* ctx, std::unique_ptr<folly::IOBuf> msg) override;

  folly::Future<folly::Unit> write(Context* ctx, std::unique_ptr<Message> msg)
      override;
};

using FetchChunkSuccessCallback = std::function<void(
    protocol::StreamChunkSlice,
    std::unique_ptr<memory::ReadOnlyByteBuffer>)>;

using FetchChunkFailureCallback = std::function<
    void(protocol::StreamChunkSlice, std::unique_ptr<std::exception>)>;

class RpcResponseCallback {
 public:
  RpcResponseCallback() = default;

  virtual ~RpcResponseCallback() = default;

  virtual void onSuccess(std::unique_ptr<memory::ReadOnlyByteBuffer>) = 0;

  virtual void onFailure(std::unique_ptr<std::exception> exception) = 0;
};

/**
 * TransportClient sends the messages to the network layer, and handles
 * the message callback, timeout, error handling, etc.
 */
class TransportClient {
 public:
  TransportClient(
      std::unique_ptr<wangle::ClientBootstrap<SerializePipeline>> client,
      std::unique_ptr<MessageDispatcher> dispatcher,
      Timeout defaultTimeout);

  virtual RpcResponse sendRpcRequestSync(const RpcRequest& request) {
    return sendRpcRequestSync(request, defaultTimeout_);
  }

  virtual RpcResponse sendRpcRequestSync(
      const RpcRequest& request,
      Timeout timeout);

  // Ignore the response, return immediately.
  virtual void sendRpcRequestWithoutResponse(const RpcRequest& request);

  virtual void pushDataAsync(
      const PushData& pushData,
      Timeout timeout,
      std::shared_ptr<RpcResponseCallback> callback);

  virtual void fetchChunkAsync(
      const protocol::StreamChunkSlice& streamChunkSlice,
      const RpcRequest& request,
      FetchChunkSuccessCallback onSuccess,
      FetchChunkFailureCallback onFailure);

  bool active() const {
    return dispatcher_->isAvailable();
  }

  ~TransportClient() = default;

 private:
  std::unique_ptr<wangle::ClientBootstrap<SerializePipeline>> client_;
  std::unique_ptr<MessageDispatcher> dispatcher_;
  Timeout defaultTimeout_;
};

class MessagePipelineFactory
    : public wangle::PipelineFactory<SerializePipeline> {
 public:
  SerializePipeline::Ptr newPipeline(
      std::shared_ptr<folly::AsyncTransport> sock) override;
};

class TransportClientFactory {
 public:
  TransportClientFactory(const std::shared_ptr<conf::CelebornConf>& conf);

  virtual std::shared_ptr<TransportClient> createClient(
      const std::string& host,
      uint16_t port);

 private:
  struct ClientPool {
    std::mutex mutex;
    std::vector<std::shared_ptr<TransportClient>> clients;
  };

  struct Hasher {
    size_t operator()(const folly::SocketAddress& lhs) const {
      return lhs.hash();
    }
  };

  int numConnectionsPerPeer_;
  folly::Synchronized<
      std::unordered_map<
          folly::SocketAddress,
          std::shared_ptr<ClientPool>,
          Hasher>,
      std::mutex>
      clientPools_;
  Timeout rpcLookupTimeout_;
  Timeout connectTimeout_;
  int numClientThreads_;
  std::shared_ptr<folly::IOThreadPoolExecutor> clientExecutor_;
};
} // namespace network
} // namespace celeborn
