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

#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/ExceptionWrapper.h>
#include <folly/Synchronized.h>

#include "celeborn/protocol/TransportMessage.h"

namespace celeborn {
namespace network {
namespace {
// Builds a retriable transport error. A closed connection or a failed socket
// write is a normal recoverable condition -- a worker restart or an idle
// timeout -- not an invariant violation, so it is reported through the promise
// instead of asserting, as the Java client does. The caller passes its own
// __FILE__/__LINE__/__FUNCTION__ so the exception points at the failing site.
folly::exception_wrapper makeRetriableTransportError(
    const char* file,
    size_t line,
    const char* function,
    const std::string& detail) {
  return folly::make_exception_wrapper<utils::CelebornRuntimeError>(
      file,
      line,
      function,
      /*expression=*/"",
      /*message=*/detail,
      utils::error_source::kErrorSourceRuntime.c_str(),
      utils::error_code::kInvalidState.c_str(),
      /*isRetriable=*/true);
}

// The reason reported on the requests that are still outstanding when the
// client goes away, either through close() or by being destroyed.
constexpr std::string_view kClientClosed = "Client closed";
} // namespace

// The requests outstanding on one connection, and the flag that retires it.
//
// Both registries and the flag live under a single mutex, so that registering a
// request cannot interleave with retiring the connection: a request is either
// registered before the retirement -- which then fails it -- or refused
// outright, leaving no close-during-send window to patch up afterwards.
//
// Promises are always fulfilled after the mutex has been released: fulfilling
// one runs the caller's continuation inline, and that continuation may re-enter
// the dispatcher or drop the last reference to the TransportClient owning it.
class MessageDispatcher::ConnectionState
    : public std::enable_shared_from_this<MessageDispatcher::ConnectionState> {
 public:
  using MsgPromise = folly::Promise<std::unique_ptr<Message>>;

  struct MsgPromiseHolder {
    MsgPromise msgPromise;
    std::chrono::time_point<std::chrono::system_clock> requestTime;
  };

  bool retired() const {
    return retired_.load();
  }

  // Registers an rpc/push request and returns the future its response is
  // delivered to, or std::nullopt when the connection has already been retired
  // and must not be written to.
  std::optional<folly::Future<std::unique_ptr<Message>>> registerRequest(
      long requestId);

  // The fetch counterpart of registerRequest.
  std::optional<folly::Future<std::unique_ptr<Message>>> registerFetch(
      const protocol::StreamChunkSlice& streamChunkSlice);

  // Unregisters the request a received response belongs to, or returns
  // std::nullopt when it is no longer registered -- it may have been failed,
  // drained or interrupted in the meantime.
  std::optional<MsgPromiseHolder> takeRequest(long requestId);

  // The fetch counterpart of takeRequest.
  std::optional<MsgPromiseHolder> takeFetch(
      const protocol::StreamChunkSlice& streamChunkSlice);

  // Retires the connection: marks it unavailable, so that
  // TransportClient::active() reports false and TransportClientFactory stops
  // handing it out, and fails everything outstanding on it with a retriable
  // error carrying `reason` -- the analogue of Java's
  // TransportResponseHandler#failOutstandingRequests. Idempotent.
  void retire(std::string_view reason);

 private:
  struct Registries {
    std::unordered_map<long, MsgPromiseHolder> requests;
    std::unordered_map<
        protocol::StreamChunkSlice,
        MsgPromiseHolder,
        protocol::StreamChunkSlice::Hasher>
        fetches;
  };

  folly::Synchronized<Registries, std::mutex> registries_;

  // Mutated only while `registries_` is held, so that a registration cannot
  // slip past a retirement. Read without the lock by isAvailable(), which sits
  // on the connection-reuse path of every request.
  std::atomic<bool> retired_{false};
};

std::optional<folly::Future<std::unique_ptr<Message>>>
MessageDispatcher::ConnectionState::registerRequest(long requestId) {
  using Result = std::optional<folly::Future<std::unique_ptr<Message>>>;
  return registries_.withLock([&](Registries& registries) -> Result {
    if (retired_.load()) {
      return std::nullopt;
    }
    // requestIds come from a monotonic counter, so a key can only collide if
    // the same request is sent twice. Report that rather than overwriting the
    // holder: folly throws from setInterruptHandler when a promise already has
    // one, which would escape from under this lock as a bare std::logic_error.
    auto [entry, registered] = registries.requests.try_emplace(requestId);
    CELEBORN_CHECK(
        registered,
        fmt::format("requestId {} is already outstanding", requestId));
    auto& holder = entry->second;
    holder.requestTime = std::chrono::system_clock::now();
    holder.msgPromise.setInterruptHandler(
        [requestId,
         weakState = weak_from_this()](const folly::exception_wrapper&) {
          LOG(WARNING) << "rpc request interrupted, requestId: " << requestId;
          if (auto state = weakState.lock()) {
            state->registries_.lock()->requests.erase(requestId);
          }
        });
    return holder.msgPromise.getFuture();
  });
}

std::optional<folly::Future<std::unique_ptr<Message>>>
MessageDispatcher::ConnectionState::registerFetch(
    const protocol::StreamChunkSlice& streamChunkSlice) {
  using Result = std::optional<folly::Future<std::unique_ptr<Message>>>;
  return registries_.withLock([&](Registries& registries) -> Result {
    if (retired_.load()) {
      return std::nullopt;
    }
    // See registerRequest(): a StreamChunkSlice identifies one chunk of one
    // stream, and a stream is opened per reader, so a key can only collide if
    // the same chunk is fetched twice on the same connection.
    auto [entry, registered] = registries.fetches.try_emplace(streamChunkSlice);
    CELEBORN_CHECK(
        registered,
        fmt::format(
            "streamChunkSlice {} is already outstanding",
            streamChunkSlice.toString()));
    auto& holder = entry->second;
    holder.requestTime = std::chrono::system_clock::now();
    holder.msgPromise.setInterruptHandler(
        [streamChunkSlice,
         weakState = weak_from_this()](const folly::exception_wrapper&) {
          LOG(WARNING) << "fetchChunk request interrupted, streamChunkSlice: "
                       << streamChunkSlice.toString();
          if (auto state = weakState.lock()) {
            state->registries_.lock()->fetches.erase(streamChunkSlice);
          }
        });
    return holder.msgPromise.getFuture();
  });
}

std::optional<MessageDispatcher::ConnectionState::MsgPromiseHolder>
MessageDispatcher::ConnectionState::takeRequest(long requestId) {
  using Result = std::optional<MsgPromiseHolder>;
  return registries_.withLock([&](Registries& registries) -> Result {
    auto search = registries.requests.find(requestId);
    if (search == registries.requests.end()) {
      return std::nullopt;
    }
    auto holder = std::move(search->second);
    registries.requests.erase(search);
    return std::move(holder);
  });
}

std::optional<MessageDispatcher::ConnectionState::MsgPromiseHolder>
MessageDispatcher::ConnectionState::takeFetch(
    const protocol::StreamChunkSlice& streamChunkSlice) {
  using Result = std::optional<MsgPromiseHolder>;
  return registries_.withLock([&](Registries& registries) -> Result {
    auto search = registries.fetches.find(streamChunkSlice);
    if (search == registries.fetches.end()) {
      return std::nullopt;
    }
    auto holder = std::move(search->second);
    registries.fetches.erase(search);
    return std::move(holder);
  });
}

void MessageDispatcher::ConnectionState::retire(std::string_view reason) {
  std::vector<std::pair<std::string, MsgPromiseHolder>> failures;
  const bool firstToRetire = registries_.withLock([&](Registries& registries) {
    for (auto& [requestId, holder] : registries.requests) {
      failures.emplace_back(
          fmt::format("{}, cancel ongoing requestId {}", reason, requestId),
          std::move(holder));
    }
    registries.requests.clear();
    for (auto& [streamChunkSlice, holder] : registries.fetches) {
      failures.emplace_back(
          fmt::format(
              "{}, cancel ongoing streamChunkSlice {}",
              reason,
              streamChunkSlice.toString()),
          std::move(holder));
    }
    registries.fetches.clear();
    return !retired_.exchange(true);
  });
  if (firstToRetire) {
    LOG(WARNING) << reason;
  }
  for (auto& [detail, holder] : failures) {
    LOG(WARNING) << detail;
    holder.msgPromise.setException(
        makeRetriableTransportError(__FILE__, __LINE__, __FUNCTION__, detail));
  }
}

MessageDispatcher::MessageDispatcher()
    : state_(std::make_shared<ConnectionState>()) {}

MessageDispatcher::~MessageDispatcher() {
  // The dispatcher can be destroyed with requests still outstanding: the client
  // pool replaces a client whose connection went bad while it is in use. Fail
  // those with the same retriable error close() reports, rather than letting
  // their futures surface folly's BrokenPromise, which carries no cause the
  // caller could classify.
  state_->retire(kClientClosed);
}

bool MessageDispatcher::isAvailable() {
  return !state_->retired();
}

folly::Future<folly::Unit> MessageDispatcher::writeToPipeline(
    std::unique_ptr<Message> toSendMsg,
    const std::function<void()>& onThrow) {
  try {
    return this->pipeline_->write(std::move(toSendMsg));
  } catch (...) {
    onThrow();
    throw;
  }
}

void MessageDispatcher::read(Context*, std::unique_ptr<Message> toRecvMsg) {
  // Hold the state for the whole call: fulfilling a promise below runs the
  // caller's continuation inline, and that continuation may drop the last
  // reference to the TransportClient owning this dispatcher, so nothing may
  // touch `this` once a promise may have been fulfilled.
  const auto state = state_;
  switch (toRecvMsg->type()) {
    case Message::RPC_RESPONSE: {
      RpcResponse* response = reinterpret_cast<RpcResponse*>(toRecvMsg.get());
      auto holder = state->takeRequest(response->requestId());
      if (!holder) {
        LOG(WARNING)
            << "requestId " << response->requestId()
            << " not found when handling RPC_RESPONSE. Might be outdated already, ignored.";
        return;
      }
      holder->msgPromise.setValue(std::move(toRecvMsg));
      return;
    }
    case Message::RPC_FAILURE: {
      RpcFailure* failure = reinterpret_cast<RpcFailure*>(toRecvMsg.get());
      auto holder = state->takeRequest(failure->requestId());
      if (!holder) {
        LOG(WARNING)
            << "requestId " << failure->requestId()
            << " not found when handling RPC_FAILURE. Might be outdated already, ignored.";
      }
      const std::string errorMsg = failure->errorMsg();
      LOG(ERROR) << "Rpc failed, requestId: " << failure->requestId()
                 << " errorMsg: " << errorMsg << std::endl;
      if (holder) {
        // Carry the worker's error message on the exception so the push/fetch
        // callbacks can recover the precise cause via
        // ShuffleClientImpl::getPushDataFailCause. A blank std::exception
        // would collapse every failure into the non-critical default.
        holder->msgPromise.setException(
            folly::make_exception_wrapper<std::runtime_error>(errorMsg));
      }
      return;
    }
    case Message::CHUNK_FETCH_SUCCESS: {
      ChunkFetchSuccess* success =
          reinterpret_cast<ChunkFetchSuccess*>(toRecvMsg.get());
      auto streamChunkSlice = success->streamChunkSlice();
      auto holder = state->takeFetch(streamChunkSlice);
      if (!holder) {
        LOG(WARNING)
            << "streamChunkSlice " << streamChunkSlice.toString()
            << " not found when handling CHUNK_FETCH_SUCCESS. Might be outdated already, ignored.";
        return;
      }
      holder->msgPromise.setValue(std::move(toRecvMsg));
      return;
    }
    case Message::CHUNK_FETCH_FAILURE: {
      ChunkFetchFailure* failure =
          reinterpret_cast<ChunkFetchFailure*>(toRecvMsg.get());
      auto streamChunkSlice = failure->streamChunkSlice();
      auto holder = state->takeFetch(streamChunkSlice);
      if (!holder) {
        LOG(WARNING)
            << "streamChunkSlice " << streamChunkSlice.toString()
            << " not found when handling CHUNK_FETCH_FAILURE. Might be outdated already, ignored.";
      }
      const std::string errorMsg = fmt::format(
          "fetchChunk failed, streamChunkSlice: {}, errorMsg: {}",
          streamChunkSlice.toString(),
          failure->errorMsg());
      LOG(ERROR) << errorMsg;
      if (holder) {
        // Carry the streamChunkSlice context and the worker's error message so
        // the reader's fetch-failure path sees the real cause.
        holder->msgPromise.setException(
            folly::make_exception_wrapper<std::runtime_error>(errorMsg));
      }
      return;
    }
    case Message::HEARTBEAT: {
      return;
    }
    default: {
      LOG(ERROR) << "unsupported msg for dispatcher";
    }
  }
}

folly::Future<std::unique_ptr<Message>> MessageDispatcher::operator()(
    std::unique_ptr<Message> toSendMsg) {
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
    case Message::PUSH_MERGED_DATA: {
      PushMergedData* pushMergedData =
          reinterpret_cast<PushMergedData*>(toSendMsg.get());
      requestId = pushMergedData->requestId();
      break;
    }
    default: {
      CELEBORN_FAIL("unsupported type");
    }
  }

  // Hold the state for the whole call: see read().
  const auto state = state_;
  auto future = state->registerRequest(requestId);
  if (!future) {
    // The connection has been retired. Fail with a retriable error rather than
    // asserting, so the caller's retry/failover logic can recover, and do not
    // write to a socket that is known to be dead.
    return folly::makeFuture<std::unique_ptr<Message>>(
        makeRetriableTransportError(
            __FILE__,
            __LINE__,
            __FUNCTION__,
            fmt::format(
                "connection closed before sending requestId {}", requestId)));
  }

  // Observe the write future, like Java's TransportClient does with
  // StdChannelListener. wangle's AsyncSocketHandler::write returns an
  // already-failed future when the socket is no longer good, and otherwise
  // fails it later from AsyncTransport::WriteCallback::writeErr; dropping the
  // future would leave the registered request pending until its timeout.
  const std::weak_ptr<ConnectionState> weakState = state;
  auto written = writeToPipeline(std::move(toSendMsg), [&]() {
    // A handler threw instead of returning a failed future -- the message is
    // serialized on the way down, by MessageSerializeHandler, and
    // wangle::Pipeline::write has no try/catch. That is a violation of our own
    // encoding invariants rather than a connection failure, so it keeps
    // propagating to the caller; but nothing was sent, so the request just
    // registered must not be left behind waiting for a response.
    state->takeRequest(requestId);
  });
  std::move(written).thenError(
      [weakState, requestId](const folly::exception_wrapper& e) {
        // The socket is dead, so the whole connection is retired -- and
        // everything outstanding on it failed -- before anything is reported,
        // the way StdChannelListener closes the channel before calling
        // handleFailure. That order matters: setException runs the caller's
        // continuation inline, and it may immediately ask
        // TransportClientFactory for a client, which must not be the connection
        // that just died. Once the dispatcher is gone there is nothing left to
        // retire; its requests were failed when it was destroyed.
        if (auto state = weakState.lock()) {
          state->retire(fmt::format(
              "Failed to send request {}, errorMsg: {}",
              requestId,
              e.what().toStdString()));
        }
      });
  return std::move(*future);
}

folly::Future<std::unique_ptr<Message>> MessageDispatcher::sendPushDataRequest(
    std::unique_ptr<Message> toSendMsg) {
  return (*this)(std::move(toSendMsg));
}

folly::Future<std::unique_ptr<Message>>
MessageDispatcher::sendFetchChunkRequest(
    const protocol::StreamChunkSlice& streamChunkSlice,
    std::unique_ptr<Message> toSendMsg) {
  CELEBORN_CHECK(toSendMsg->type() == Message::RPC_REQUEST);

  // Hold the state for the whole call: see read().
  const auto state = state_;
  auto future = state->registerFetch(streamChunkSlice);
  if (!future) {
    // The connection has been retired: fail retriably rather than asserting, so
    // CelebornInputStream can retry or fail over to a replica.
    return folly::makeFuture<std::unique_ptr<Message>>(
        makeRetriableTransportError(
            __FILE__,
            __LINE__,
            __FUNCTION__,
            fmt::format(
                "connection closed before fetching streamChunkSlice {}",
                streamChunkSlice.toString())));
  }

  // Write-failure handling: see operator().
  const std::weak_ptr<ConnectionState> weakState = state;
  auto written = writeToPipeline(
      std::move(toSendMsg), [&]() { state->takeFetch(streamChunkSlice); });
  std::move(written).thenError(
      [weakState, streamChunkSlice](const folly::exception_wrapper& e) {
        if (auto state = weakState.lock()) {
          state->retire(fmt::format(
              "Failed to send request for streamChunkSlice {}, errorMsg: {}",
              streamChunkSlice.toString(),
              e.what().toStdString()));
        }
      });
  return std::move(*future);
}

void MessageDispatcher::sendRpcRequestWithoutResponse(
    std::unique_ptr<Message> toSendMsg) {
  CELEBORN_CHECK(toSendMsg->type() == Message::RPC_REQUEST);
  const long requestId =
      reinterpret_cast<RpcRequest*>(toSendMsg.get())->requestId();

  // Hold the state for the whole call: see read().
  const auto state = state_;
  if (state->retired()) {
    // There is no promise to fail for this send -- the caller does not wait for
    // a response -- and no live socket to write to either.
    LOG(WARNING) << "connection closed before sending requestId " << requestId
                 << " without response";
    return;
  }

  // Unlike the paths above, this one cannot make the check and the send atomic:
  // there is no promise to register, so nothing to register it against. Losing
  // the race is harmless -- the write then fails and retires the connection
  // below -- it just means the send reached a socket already known to be dead.
  //
  // A failed write leaves nothing to fail here either, but it still means the
  // connection is dead, so it has to be retired all the same: otherwise the
  // client pool keeps handing it to the next caller.
  const std::weak_ptr<ConnectionState> weakState = state;
  folly::Future<folly::Unit> written = folly::makeFuture();
  try {
    written = this->pipeline_->write(std::move(toSendMsg));
  } catch (const std::exception& e) {
    // A handler rejected the message by throwing -- see writeToPipeline(). With
    // no promise and no caller waiting, logging is the only way to report it,
    // and it must not propagate: ~WorkerPartitionReader sends BufferStreamEnd
    // this way, and an exception escaping a destructor aborts the process.
    LOG(ERROR) << "failed to send requestId " << requestId
               << " without response, errorMsg: " << e.what();
    return;
  } catch (...) {
    // A handler is free to throw something that does not derive from
    // std::exception, and that must not escape a destructor either.
    LOG(ERROR) << "failed to send requestId " << requestId
               << " without response, errorMsg: unknown exception";
    return;
  }
  std::move(written).thenError(
      [weakState, requestId](const folly::exception_wrapper& e) {
        if (auto state = weakState.lock()) {
          state->retire(fmt::format(
              "Failed to send request {} without response, errorMsg: {}",
              requestId,
              e.what().toStdString()));
        }
      });
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
  // Hold the state for the whole call: see read().
  const auto state = state_;
  // Close the pipeline before failing what was outstanding on it: the order
  // Java uses, and the only safe one here, since retiring fulfils promises
  // inline and a continuation may drop the last reference to the
  // TransportClient owning this dispatcher.
  auto result = ClientDispatcherBase::close();
  state->retire(kClientClosed);
  return result;
}

folly::Future<folly::Unit> MessageDispatcher::close(Context* ctx) {
  // Ordering: see close().
  const auto state = state_;
  auto result = ClientDispatcherBase::close(ctx);
  state->retire(kClientClosed);
  return result;
}
} // namespace network
} // namespace celeborn
