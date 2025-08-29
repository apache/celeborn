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

#include "celeborn/network/MessageDispatcher.h"

#include "celeborn/protocol/TransportMessage.h"

namespace celeborn {
namespace network {
void MessageDispatcher::read(Context*, std::unique_ptr<Message> toRecvMsg) {
  switch (toRecvMsg->type()) {
    case Message::RPC_RESPONSE: {
      RpcResponse* response = reinterpret_cast<RpcResponse*>(toRecvMsg.get());
      bool found = true;
      auto holder = requestIdRegistry_.withLock([&](auto& registry) {
        auto search = registry.find(response->requestId());
        if (search == registry.end()) {
          LOG(WARNING)
              << "requestId " << response->requestId()
              << " not found when handling RPC_RESPONSE. Might be outdated already, ignored.";
          found = false;
          return MsgPromiseHolder{};
        }
        auto result = std::move(search->second);
        registry.erase(response->requestId());
        return std::move(result);
      });
      if (found) {
        holder.msgPromise.setValue(std::move(toRecvMsg));
      }
      return;
    }
    case Message::RPC_FAILURE: {
      RpcFailure* failure = reinterpret_cast<RpcFailure*>(toRecvMsg.get());
      bool found = true;
      auto holder = requestIdRegistry_.withLock([&](auto& registry) {
        auto search = registry.find(failure->requestId());
        if (search == registry.end()) {
          LOG(WARNING)
              << "requestId " << failure->requestId()
              << " not found when handling RPC_FAILURE. Might be outdated already, ignored.";
          found = false;
          return MsgPromiseHolder{};
        }
        auto result = std::move(search->second);
        registry.erase(failure->requestId());
        return std::move(result);
      });
      LOG(ERROR) << "Rpc failed, requestId: " << failure->requestId()
                 << " errorMsg: " << failure->errorMsg() << std::endl;
      if (found) {
        holder.msgPromise.setException(
            folly::exception_wrapper(std::exception()));
      }
      return;
    }
    case Message::CHUNK_FETCH_SUCCESS: {
      ChunkFetchSuccess* success =
          reinterpret_cast<ChunkFetchSuccess*>(toRecvMsg.get());
      auto streamChunkSlice = success->streamChunkSlice();
      bool found = true;
      auto holder = streamChunkSliceRegistry_.withLock([&](auto& registry) {
        auto search = registry.find(streamChunkSlice);
        if (search == registry.end()) {
          LOG(WARNING)
              << "streamChunkSlice " << streamChunkSlice.toString()
              << " not found when handling CHUNK_FETCH_SUCCESS. Might be outdated already, ignored.";
          found = false;
          return MsgPromiseHolder{};
        }
        auto result = std::move(search->second);
        registry.erase(streamChunkSlice);
        return std::move(result);
      });
      if (found) {
        holder.msgPromise.setValue(std::move(toRecvMsg));
      }
      return;
    }
    case Message::CHUNK_FETCH_FAILURE: {
      ChunkFetchFailure* failure =
          reinterpret_cast<ChunkFetchFailure*>(toRecvMsg.get());
      auto streamChunkSlice = failure->streamChunkSlice();
      bool found = true;
      auto holder = streamChunkSliceRegistry_.withLock([&](auto& registry) {
        auto search = registry.find(streamChunkSlice);
        if (search == registry.end()) {
          LOG(WARNING)
              << "streamChunkSlice " << streamChunkSlice.toString()
              << " not found when handling CHUNK_FETCH_FAILURE. Might be outdated already, ignored.";
          found = false;
          return MsgPromiseHolder{};
        }
        auto result = std::move(search->second);
        registry.erase(streamChunkSlice);
        return std::move(result);
      });
      std::string errorMsg = fmt::format(
          "fetchChunk failed, streamChunkSlice: {}, errorMsg: {}",
          streamChunkSlice.toString(),
          failure->errorMsg());
      LOG(ERROR) << errorMsg;
      if (found) {
        holder.msgPromise.setException(
            folly::exception_wrapper(std::exception()));
      }
      return;
    }
    default: {
      LOG(ERROR) << "unsupported msg for dispatcher";
    }
  }
}

folly::Future<std::unique_ptr<Message>> MessageDispatcher::operator()(
    std::unique_ptr<Message> toSendMsg) {
  CELEBORN_CHECK(!closed_);
  auto currTime = std::chrono::system_clock::now();
  long requestId;
  switch (toSendMsg->type()) {
    case Message::RPC_REQUEST: {
      RpcRequest* request = reinterpret_cast<RpcRequest*>(toSendMsg.get());
      requestId = request->requestId();
      break;
    }
    case Message::PUSH_DATA: {
      PushData* pushData = reinterpret_cast<PushData*>(toSendMsg.get());
      requestId = pushData->requestId();
      break;
    }
    default: {
      CELEBORN_FAIL("unsupported type");
    }
  }

  auto f = requestIdRegistry_.withLock(
      [&](auto& registry) -> folly::Future<std::unique_ptr<Message>> {
        auto& holder = registry[requestId];
        holder.requestTime = currTime;
        auto& p = holder.msgPromise;
        p.setInterruptHandler([requestId,
                               this](const folly::exception_wrapper&) {
          this->requestIdRegistry_.lock()->erase(requestId);
          LOG(WARNING) << "rpc request interrupted, requestId: " << requestId;
        });
        return p.getFuture();
      });

  this->pipeline_->write(std::move(toSendMsg));

  CELEBORN_CHECK(!closed_);
  return f;
}

folly::Future<std::unique_ptr<Message>>
MessageDispatcher::sendPushDataRequest(std::unique_ptr<Message> toSendMsg) {
  return (*this)(std::move(toSendMsg));
}

folly::Future<std::unique_ptr<Message>>
MessageDispatcher::sendFetchChunkRequest(
    const protocol::StreamChunkSlice& streamChunkSlice,
    std::unique_ptr<Message> toSendMsg) {
  CELEBORN_CHECK(!closed_);
  CELEBORN_CHECK(toSendMsg->type() == Message::RPC_REQUEST);
  auto f = streamChunkSliceRegistry_.withLock([&](auto& registry) {
    auto& holder = registry[streamChunkSlice];
    holder.requestTime = std::chrono::system_clock::now();
    auto& p = holder.msgPromise;
    p.setInterruptHandler(
        [streamChunkSlice, this](const folly::exception_wrapper&) {
          LOG(WARNING) << "fetchChunk request interrupted, "
                          "streamChunkSlice: "
                       << streamChunkSlice.toString();
          this->streamChunkSliceRegistry_.lock()->erase(streamChunkSlice);
        });
    return p.getFuture();
  });
  this->pipeline_->write(std::move(toSendMsg));
  CELEBORN_CHECK(!closed_);
  return f;
}

void MessageDispatcher::sendRpcRequestWithoutResponse(
    std::unique_ptr<Message> toSendMsg) {
  CELEBORN_CHECK(toSendMsg->type() == Message::RPC_REQUEST);
  this->pipeline_->write(std::move(toSendMsg));
}

void MessageDispatcher::readEOF(Context* ctx) {
  LOG(ERROR) << "readEOF, start to close client";
  ctx->fireReadEOF();
  close();
}

void MessageDispatcher::readException(
    Context* ctx,
    folly::exception_wrapper e) {
  LOG(ERROR) << "readException: " << e.what() << " , start to close client";
  ctx->fireReadException(std::move(e));
  close();
}

void MessageDispatcher::transportActive(Context* ctx) {
  // Typically do nothing.
  ctx->fireTransportActive();
}

void MessageDispatcher::transportInactive(Context* ctx) {
  LOG(ERROR) << "transportInactive, start to close client";
  ctx->fireTransportInactive();
  close();
}

folly::Future<folly::Unit> MessageDispatcher::writeException(
    Context* ctx,
    folly::exception_wrapper e) {
  LOG(ERROR) << "writeException: " << e.what() << " , start to close client";
  auto result = ctx->fireWriteException(std::move(e));
  close();
  return result;
}

folly::Future<folly::Unit> MessageDispatcher::close() {
  if (!closed_) {
    closed_ = true;
    cleanup();
  }
  return ClientDispatcherBase::close();
}

folly::Future<folly::Unit> MessageDispatcher::close(Context* ctx) {
  if (!closed_) {
    closed_ = true;
    cleanup();
  }

  return ClientDispatcherBase::close(ctx);
}

void MessageDispatcher::cleanup() {
  LOG(WARNING) << "Cleaning up client!";
  requestIdRegistry_.withLock([&](auto& registry) {
    for (auto& [requestId, promiseHolder] : registry) {
      auto errorMsg =
          fmt::format("Client closed, cancel ongoing requestId {}", requestId);
      LOG(WARNING) << errorMsg;
      promiseHolder.msgPromise.setException(std::runtime_error(errorMsg));
    }
    registry.clear();
  });
  streamChunkSliceRegistry_.withLock([&](auto& registry) {
    for (auto& [streamChunkSlice, promiseHolder] : registry) {
      auto errorMsg = fmt::format(
          "Client closed, cancel ongoing streamChunkSlice {}",
          streamChunkSlice.toString());
      LOG(WARNING) << errorMsg;
      promiseHolder.msgPromise.setException(std::runtime_error(errorMsg));
    }
    registry.clear();
  });
}
} // namespace network
} // namespace celeborn
