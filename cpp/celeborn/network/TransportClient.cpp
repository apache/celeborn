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

#include "celeborn/network/TransportClient.h"

#include "celeborn/network/FrameDecoder.h"
#include "celeborn/protocol/TransportMessage.h"

namespace celeborn {
namespace network {
namespace {
// True when the cause was already classified as retriable by the transport.
bool isRetriableCause(const std::exception& e) {
  const auto* celebornException =
      dynamic_cast<const utils::CelebornException*>(&e);
  return celebornException != nullptr && celebornException->isRetriable();
}

// Rebuilds the failure handed to a push/fetch callback. The transport marks a
// recoverable failure -- a closed connection, a failed write, a connect
// failure -- with a retriable CelebornException; flattening it into a plain
// std::runtime_error here would drop that classification before any caller can
// observe it, so the retriable exception is forwarded as-is.
//
// Matching CelebornRuntimeError alone is enough: the CELEBORN_CHECK /
// CELEBORN_FAIL macros all hardcode isRetriable=false, so a retriable cause is
// always one of the CelebornRuntimeErrors the transport builds explicitly.
std::unique_ptr<std::exception> toCallbackException(
    const folly::exception_wrapper& e) {
  std::unique_ptr<std::exception> retriableFailure;
  e.with_exception([&](const utils::CelebornRuntimeError& error) {
    if (error.isRetriable()) {
      retriableFailure = std::make_unique<utils::CelebornRuntimeError>(error);
    }
  });
  if (retriableFailure) {
    return retriableFailure;
  }
  return std::make_unique<std::runtime_error>(e.what().toStdString());
}

// The counterpart of toCallbackException for a synchronously thrown cause:
// wraps `errorMsg` while keeping the cause's retriable classification.
std::unique_ptr<std::exception> wrapCallbackException(
    const char* file,
    size_t line,
    const char* function,
    const std::string& errorMsg,
    const std::exception& cause) {
  if (!isRetriableCause(cause)) {
    return std::make_unique<std::runtime_error>(errorMsg);
  }
  return std::make_unique<utils::CelebornRuntimeError>(
      file,
      line,
      function,
      /*expression=*/"",
      /*message=*/errorMsg,
      utils::error_source::kErrorSourceRuntime.c_str(),
      utils::error_code::kInvalidState.c_str(),
      /*isRetriable=*/true);
}

// Rethrows `errorMsg` preserving the cause's retriable classification.
// CELEBORN_FAIL would hardcode isRetriable=false and hide a recoverable
// transport failure from the retry/failover paths.
[[noreturn]] void failPreservingRetriable(
    const char* file,
    size_t line,
    const char* function,
    const std::string& errorMsg,
    const std::exception& cause) {
  throw utils::CelebornRuntimeError(
      file,
      line,
      function,
      /*expression=*/"",
      /*message=*/errorMsg,
      utils::error_source::kErrorSourceRuntime.c_str(),
      utils::error_code::kInvalidState.c_str(),
      /*isRetriable=*/isRetriableCause(cause));
}
} // namespace

void MessageSerializeHandler::read(
    Context* ctx,
    std::unique_ptr<folly::IOBuf> msg) {
  auto buffer = memory::ByteBuffer::createReadOnly(std::move(msg));
  ctx->fireRead(Message::decodeFrom(std::move(buffer)));
}

folly::Future<folly::Unit> MessageSerializeHandler::write(
    Context* ctx,
    std::unique_ptr<Message> msg) {
  return ctx->fireWrite(msg->encode()->getData());
}

TransportClient::TransportClient(
    std::unique_ptr<wangle::ClientBootstrap<SerializePipeline>> client,
    std::unique_ptr<MessageDispatcher> dispatcher,
    Timeout defaultTimeout)
    : client_(std::move(client)),
      dispatcher_(std::move(dispatcher)),
      defaultTimeout_(defaultTimeout) {}

RpcResponse TransportClient::sendRpcRequestSync(
    const RpcRequest& request,
    Timeout timeout) {
  try {
    auto requestMsg = std::make_unique<RpcRequest>(request);
    auto future = dispatcher_->sendRpcRequest(std::move(requestMsg));
    auto responseMsg = std::move(future).get(timeout);
    CELEBORN_CHECK(
        responseMsg->type() == Message::RPC_RESPONSE,
        "responseMsg type should be RPC_RESPONSE");
    return RpcResponse(*reinterpret_cast<RpcResponse*>(responseMsg.get()));
  } catch (const std::exception& e) {
    std::string errorMsg = fmt::format(
        "sendRpc failure, requestId: {}, timeout: {}, errorMsg: {}",
        request.requestId(),
        timeout,
        folly::exceptionStr(e).toStdString());
    LOG(ERROR) << errorMsg;
    failPreservingRetriable(__FILE__, __LINE__, __FUNCTION__, errorMsg, e);
  }
}

void TransportClient::sendRpcRequestWithoutResponse(const RpcRequest& request) {
  try {
    auto requestMsg = std::make_unique<RpcRequest>(request);
    dispatcher_->sendRpcRequestWithoutResponse(std::move(requestMsg));
  } catch (const std::exception& e) {
    std::string errorMsg = fmt::format(
        "sendRpc failure, requestId: {}, errorMsg: {}",
        request.requestId(),
        folly::exceptionStr(e).toStdString());
    LOG(ERROR) << errorMsg;
    CELEBORN_FAIL(errorMsg);
  }
}

void TransportClient::pushDataAsync(
    const PushData& pushData,
    Timeout timeout,
    std::shared_ptr<RpcResponseCallback> callback) {
  try {
    auto requestMsg = std::make_unique<PushData>(pushData);
    auto future = dispatcher_->sendPushDataRequest(std::move(requestMsg));
    std::move(future)
        .within(timeout)
        .thenValue(
            [_callback = callback](std::unique_ptr<Message> responseMsg) {
              if (responseMsg->type() == Message::RPC_RESPONSE) {
                auto rpcResponse =
                    reinterpret_cast<RpcResponse*>(responseMsg.get());
                _callback->onSuccess(rpcResponse->body());
              } else {
                _callback->onFailure(std::make_unique<std::runtime_error>(
                    "pushData return value type is not rpcResponse"));
              }
            })
        .thenError([_callback = callback](const folly::exception_wrapper& e) {
          _callback->onFailure(toCallbackException(e));
        });

  } catch (std::exception& e) {
    auto errorMsg = fmt::format(
        "PushData failed. shuffleKey: {}, partitionUniqueId: {}, mode: {}, error message: {}",
        pushData.shuffleKey(),
        pushData.partitionUniqueId(),
        pushData.mode(),
        e.what());
    LOG(ERROR) << errorMsg;
    callback->onFailure(
        wrapCallbackException(__FILE__, __LINE__, __FUNCTION__, errorMsg, e));
  }
}

void TransportClient::pushMergedDataAsync(
    const PushMergedData& pushMergedData,
    Timeout timeout,
    std::shared_ptr<RpcResponseCallback> callback) {
  try {
    auto requestMsg = std::make_unique<PushMergedData>(pushMergedData);
    auto future = dispatcher_->sendPushDataRequest(std::move(requestMsg));
    std::move(future)
        .within(timeout)
        .thenValue(
            [_callback = callback](std::unique_ptr<Message> responseMsg) {
              if (responseMsg->type() == Message::RPC_RESPONSE) {
                auto rpcResponse =
                    reinterpret_cast<RpcResponse*>(responseMsg.get());
                _callback->onSuccess(rpcResponse->body());
              } else {
                _callback->onFailure(std::make_unique<std::runtime_error>(
                    "pushMergedData return value type is not rpcResponse"));
              }
            })
        .thenError([_callback = callback](const folly::exception_wrapper& e) {
          _callback->onFailure(toCallbackException(e));
        });

  } catch (std::exception& e) {
    auto errorMsg = fmt::format(
        "PushMergedData failed. shuffleKey: {}, mode: {}, error message: {}",
        pushMergedData.shuffleKey(),
        pushMergedData.mode(),
        e.what());
    LOG(ERROR) << errorMsg;
    callback->onFailure(
        wrapCallbackException(__FILE__, __LINE__, __FUNCTION__, errorMsg, e));
  }
}

void TransportClient::fetchChunkAsync(
    const protocol::StreamChunkSlice& streamChunkSlice,
    const RpcRequest& request,
    FetchChunkSuccessCallback onSuccess,
    FetchChunkFailureCallback onFailure) {
  try {
    auto requestMsg = std::make_unique<RpcRequest>(request);
    auto future = dispatcher_->sendFetchChunkRequest(
        streamChunkSlice, std::move(requestMsg));
    std::move(future)
        .thenValue([=, _onSuccess = onSuccess, _onFailure = onFailure](
                       std::unique_ptr<Message> responseMsg) {
          if (responseMsg->type() == Message::CHUNK_FETCH_SUCCESS) {
            auto chunkFetchSuccess =
                reinterpret_cast<ChunkFetchSuccess*>(responseMsg.get());
            _onSuccess(streamChunkSlice, chunkFetchSuccess->body());
          } else {
            _onFailure(
                streamChunkSlice,
                std::make_unique<std::runtime_error>(fmt::format(
                    "chunk fetch of streamChunkSlice {} does not succeed",
                    streamChunkSlice.toString())));
          }
        })
        .thenError(
            [=, _onFailure = onFailure](const folly::exception_wrapper& e) {
              _onFailure(streamChunkSlice, toCallbackException(e));
            });
  } catch (std::exception& e) {
    LOG(ERROR) << "fetchChunk failed. streamChunkSlice: "
               << streamChunkSlice.toString() << ", errorMsg: " << e.what();
    failPreservingRetriable(__FILE__, __LINE__, __FUNCTION__, e.what(), e);
  }
}

SerializePipeline::Ptr MessagePipelineFactory::newPipeline(
    std::shared_ptr<folly::AsyncTransport> sock) {
  auto pipeline = SerializePipeline::create();
  pipeline->addBack(wangle::AsyncSocketHandler(sock));
  // Ensure we can write from any thread.
  pipeline->addBack(wangle::EventBaseHandler());
  pipeline->addBack(FrameDecoder());
  pipeline->addBack(MessageSerializeHandler());
  pipeline->finalize();

  return pipeline;
}

TransportClientFactory::TransportClientFactory(
    const std::shared_ptr<const conf::CelebornConf>& conf) {
  numConnectionsPerPeer_ = conf->networkIoNumConnectionsPerPeer();
  rpcLookupTimeout_ = conf->rpcLookupTimeout();
  connectTimeout_ = conf->networkConnectTimeout();
  numClientThreads_ = conf->networkIoClientThreads();
  if (numClientThreads_ <= 0) {
    numClientThreads_ = std::thread::hardware_concurrency() * 2;
  }
  clientExecutor_ =
      std::make_shared<folly::IOThreadPoolExecutor>(numClientThreads_);
}

std::shared_ptr<TransportClient> TransportClientFactory::createClient(
    const std::string& host,
    uint16_t port) {
  return createClient(host, port, std::rand());
}

std::shared_ptr<TransportClient> TransportClientFactory::createClient(
    const std::string& host,
    uint16_t port,
    int32_t partitionId) {
  auto address = folly::SocketAddress(host, port, true);
  auto pool = clientPools_.withLock([&](auto& registry) {
    auto iter = registry.find(address);
    if (iter != registry.end()) {
      return iter->second;
    }
    auto createdPool = std::make_shared<ClientPool>();
    createdPool->clients.resize(numConnectionsPerPeer_);
    registry[address] = createdPool;
    return createdPool;
  });
  auto clientId = partitionId % numConnectionsPerPeer_;
  {
    std::lock_guard<std::mutex> lock(pool->mutex);
    // TODO: auto-disconnect if the connection is idle for a long time?
    if (pool->clients[clientId] && pool->clients[clientId]->active()) {
      VLOG(1) << "reusing client for address host " << host << " port " << port;
      return pool->clients[clientId];
    }

    auto bootstrap =
        std::make_unique<wangle::ClientBootstrap<SerializePipeline>>();
    bootstrap->group(clientExecutor_);
    bootstrap->pipelineFactory(std::make_shared<MessagePipelineFactory>());
    try {
      auto pipeline = bootstrap->connect(folly::SocketAddress(host, port, true))
                          .get(rpcLookupTimeout_);

      auto dispatcher = std::make_unique<MessageDispatcher>();
      dispatcher->setPipeline(pipeline);
      pool->clients[clientId] = std::make_shared<TransportClient>(
          std::move(bootstrap), std::move(dispatcher), connectTimeout_);
      return pool->clients[clientId];
    } catch (const std::exception& e) {
      std::string errorMsg = fmt::format(
          "connect to server failure, serverAddr: {}:{}, timeout: {}, errorMsg: {}",
          host,
          port,
          connectTimeout_,
          folly::exceptionStr(e).toStdString());
      LOG(ERROR) << errorMsg;
      // Failing to establish a connection is transient: the peer may be
      // restarting, or the network may be briefly unavailable. Classify it as
      // retriable rather than as an invariant violation, so that the
      // createReaderWithRetry and push failover paths can tell it apart from a
      // terminal failure.
      throw utils::CelebornRuntimeError(
          __FILE__,
          __LINE__,
          __FUNCTION__,
          /*expression=*/"",
          /*message=*/errorMsg,
          utils::error_source::kErrorSourceRuntime.c_str(),
          utils::error_code::kInvalidState.c_str(),
          /*isRetriable=*/true);
    }
  }
}
} // namespace network
} // namespace celeborn
