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

package org.apache.celeborn.common.network.client;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.protocol.*;
import org.apache.celeborn.common.network.server.MessageHandler;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.read.FetchRequestInfo;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.common.write.PushRequestInfo;

/**
 * Handler that processes server responses, in response to requests issued from a
 * [[TransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
 *
 * <p>Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
  private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

  private final TransportConf conf;
  private final Channel channel;

  private final ConcurrentHashMap<StreamChunkSlice, FetchRequestInfo> outstandingFetches;

  private final ConcurrentHashMap<Long, RpcResponseCallback> outstandingRpcs;
  private final ConcurrentHashMap<Long, PushRequestInfo> outstandingPushes;

  /** Records the time (in system nanoseconds) that the last fetch or RPC request was sent. */
  private final AtomicLong timeOfLastRequestNs;

  private final long pushTimeoutCheckerInterval;
  private static ScheduledExecutorService pushTimeoutChecker = null;
  private ScheduledFuture pushCheckerScheduleFuture;

  private final long fetchTimeoutCheckerInterval;
  private static ScheduledExecutorService fetchTimeoutChecker = null;
  private ScheduledFuture fetchCheckerScheduleFuture;

  public TransportResponseHandler(TransportConf conf, Channel channel) {
    this.conf = conf;
    this.channel = channel;
    this.outstandingFetches = JavaUtils.newConcurrentHashMap();
    this.outstandingRpcs = JavaUtils.newConcurrentHashMap();
    this.outstandingPushes = JavaUtils.newConcurrentHashMap();
    this.timeOfLastRequestNs = new AtomicLong(0);
    this.pushTimeoutCheckerInterval = conf.pushDataTimeoutCheckIntervalMs();
    this.fetchTimeoutCheckerInterval = conf.fetchDataTimeoutCheckIntervalMs();

    String module = conf.getModuleName();
    boolean checkPushTimeout = false;
    boolean checkFetchTimeout = false;
    if (TransportModuleConstants.DATA_MODULE.equals(module)) {
      checkPushTimeout = true;
      checkFetchTimeout = true;
    } else if (TransportModuleConstants.PUSH_MODULE.equals(module)) {
      checkPushTimeout = true;
    }
    synchronized (TransportResponseHandler.class) {
      if (checkPushTimeout) {
        if (pushTimeoutChecker == null) {
          pushTimeoutChecker =
              ThreadUtils.newDaemonThreadPoolScheduledExecutor(
                  "push-timeout-checker", conf.pushDataTimeoutCheckerThreads());
        }
      }

      if (checkFetchTimeout) {
        if (fetchTimeoutChecker == null) {
          fetchTimeoutChecker =
              ThreadUtils.newDaemonThreadPoolScheduledExecutor(
                  "fetch-timeout-checker", conf.fetchDataTimeoutCheckerThreads());
        }
      }
    }

    if (checkPushTimeout) {
      pushCheckerScheduleFuture =
          pushTimeoutChecker.scheduleWithFixedDelay(
              () -> failExpiredPushRequest(),
              pushTimeoutCheckerInterval,
              pushTimeoutCheckerInterval,
              TimeUnit.MILLISECONDS);
    }

    if (checkFetchTimeout) {
      fetchCheckerScheduleFuture =
          fetchTimeoutChecker.scheduleWithFixedDelay(
              () -> failExpiredFetchRequest(),
              fetchTimeoutCheckerInterval,
              fetchTimeoutCheckerInterval,
              TimeUnit.MILLISECONDS);
    }
  }

  public void failExpiredPushRequest() {
    long currentTime = System.currentTimeMillis();
    Iterator<Map.Entry<Long, PushRequestInfo>> iter = outstandingPushes.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<Long, PushRequestInfo> entry = iter.next();
      if (entry.getValue().dueTime <= currentTime) {
        PushRequestInfo info = removePushRequest(entry.getKey());
        if (info != null) {
          if (info.channelFuture != null) {
            info.channelFuture.cancel(true);
          }
          // When module name equals to DATA_MODULE, mean shuffle client push data, else means
          // do data replication.
          if (TransportModuleConstants.DATA_MODULE.equals(conf.getModuleName())) {
            info.callback.onFailure(new CelebornIOException(StatusCode.PUSH_DATA_TIMEOUT_PRIMARY));
          } else if (TransportModuleConstants.PUSH_MODULE.equals(conf.getModuleName())) {
            info.callback.onFailure(new CelebornIOException(StatusCode.PUSH_DATA_TIMEOUT_REPLICA));
          }
          info.channelFuture = null;
          info.callback = null;
        }
      }
    }
  }

  public void failExpiredFetchRequest() {
    long currentTime = System.currentTimeMillis();
    Iterator<Map.Entry<StreamChunkSlice, FetchRequestInfo>> iter =
        outstandingFetches.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<StreamChunkSlice, FetchRequestInfo> entry = iter.next();
      if (entry.getValue().dueTime <= currentTime) {
        FetchRequestInfo info = removeFetchRequest(entry.getKey());
        if (info != null) {
          if (info.channelFuture != null) {
            info.channelFuture.cancel(true);
          }
          logger.info(
              "Fail expire fetch request {},{},{},{}",
              entry.getKey().streamId,
              entry.getKey().chunkIndex,
              entry.getKey().offset,
              entry.getKey().len);
          info.callback.onFailure(
              entry.getKey().chunkIndex, new CelebornIOException(StatusCode.FETCH_DATA_TIMEOUT));
          info.channelFuture = null;
          info.callback = null;
        }
      }
    }
  }

  public void addFetchRequest(StreamChunkSlice streamChunkSlice, FetchRequestInfo info) {
    updateTimeOfLastRequest();
    if (outstandingFetches.containsKey(streamChunkSlice)) {
      logger.warn("[addFetchRequest] streamChunkSlice {} already exists!", streamChunkSlice);
    }
    outstandingFetches.put(streamChunkSlice, info);
  }

  public FetchRequestInfo removeFetchRequest(StreamChunkSlice streamChunkSlice) {
    return outstandingFetches.remove(streamChunkSlice);
  }

  public void addRpcRequest(long requestId, RpcResponseCallback callback) {
    updateTimeOfLastRequest();
    if (outstandingRpcs.containsKey(requestId)) {
      logger.warn("[addRpcRequest] requestId {} already exists!", requestId);
    }
    outstandingRpcs.put(requestId, callback);
  }

  public RpcResponseCallback removeRpcRequest(long requestId) {
    return outstandingRpcs.remove(requestId);
  }

  public void addPushRequest(long requestId, PushRequestInfo info) {
    updateTimeOfLastRequest();
    if (outstandingPushes.containsKey(requestId)) {
      logger.warn("[addPushRequest] requestId {} already exists!", requestId);
    }
    outstandingPushes.put(requestId, info);
  }

  public PushRequestInfo removePushRequest(long requestId) {
    return outstandingPushes.remove(requestId);
  }

  /**
   * Fire the failure callback for all outstanding requests. This is called when we have an uncaught
   * exception or pre-mature connection termination.
   */
  private void failOutstandingRequests(Throwable cause) {
    Iterator<StreamChunkSlice> fetchRequestIter = outstandingFetches.keySet().iterator();
    while (fetchRequestIter.hasNext()) {
      try {
        StreamChunkSlice slice = fetchRequestIter.next();
        FetchRequestInfo info = removeFetchRequest(slice);
        if (info != null) {
          info.callback.onFailure(slice.chunkIndex, cause);
        }
      } catch (Exception e) {
        logger.warn("ChunkReceivedCallback.onFailure throws exception", e);
      }
    }

    Iterator<Long> rpcRequestIter = outstandingRpcs.keySet().iterator();
    while (rpcRequestIter.hasNext()) {
      try {
        long requestId = rpcRequestIter.next();
        RpcResponseCallback callback = removeRpcRequest(requestId);
        if (callback != null) {
          callback.onFailure(cause);
        }
      } catch (Exception e) {
        logger.warn("RpcResponseCallback.onFailure throws exception", e);
      }
    }

    Iterator<Long> pushRequestIter = outstandingPushes.keySet().iterator();
    while (pushRequestIter.hasNext()) {
      try {
        long requestId = pushRequestIter.next();
        PushRequestInfo info = removePushRequest(requestId);
        if (info != null) {
          info.callback.onFailure(cause);
        }
      } catch (Exception e) {
        logger.warn("RpcResponseCallback.onFailure throws exception", e);
      }
    }
  }

  @Override
  public void channelActive() {}

  @Override
  public void channelInactive() {
    if (numOutstandingRequests() > 0) {
      // show the details of outstanding Fetches
      if (logger.isDebugEnabled()) {
        if (outstandingFetches.size() > 0) {
          for (Map.Entry<StreamChunkSlice, FetchRequestInfo> e : outstandingFetches.entrySet()) {
            StreamChunkSlice key = e.getKey();
            logger.debug("The channel is closed, but there is still outstanding Fetch {}", key);
          }
        } else {
          logger.debug("The channel is closed, the outstanding Fetches are empty");
        }
      }
      String remoteAddress = NettyUtils.getRemoteAddress(channel);
      logger.error(
          "Still have {} requests outstanding when connection from {} is closed",
          numOutstandingRequests(),
          remoteAddress);
      failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
    }
    if (pushCheckerScheduleFuture != null) {
      pushCheckerScheduleFuture.cancel(false);
    }
    if (fetchCheckerScheduleFuture != null) {
      fetchCheckerScheduleFuture.cancel(false);
    }
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = NettyUtils.getRemoteAddress(channel);
      logger.error(
          "Still have {} requests outstanding when connection from {} is closed",
          numOutstandingRequests(),
          remoteAddress);
      failOutstandingRequests(cause);
    }
    if (pushCheckerScheduleFuture != null) {
      pushCheckerScheduleFuture.cancel(false);
    }
    if (fetchCheckerScheduleFuture != null) {
      fetchCheckerScheduleFuture.cancel(false);
    }
  }

  @Override
  public void handle(ResponseMessage message) throws Exception {
    if (message instanceof ChunkFetchSuccess) {
      ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
      logger.debug("Chunk {} fetch succeeded", resp.streamChunkSlice);
      FetchRequestInfo info = removeFetchRequest(resp.streamChunkSlice);
      if (info == null) {
        logger.warn(
            "Ignoring response for block {} from {} since it is not outstanding",
            resp.streamChunkSlice,
            NettyUtils.getRemoteAddress(channel));
        resp.body().release();
      } else {
        try {
          info.callback.onSuccess(resp.streamChunkSlice.chunkIndex, resp.body());
        } finally {
          resp.body().release();
        }
      }
    } else if (message instanceof ChunkFetchFailure) {
      ChunkFetchFailure resp = (ChunkFetchFailure) message;
      logger.error(
          "chunk {} fetch failed, errorMessage {}", resp.streamChunkSlice, resp.errorString);
      FetchRequestInfo info = removeFetchRequest(resp.streamChunkSlice);
      if (info == null) {
        logger.warn(
            "Ignoring response for block {} from {} ({}) since it is not outstanding",
            resp.streamChunkSlice,
            NettyUtils.getRemoteAddress(channel),
            resp.errorString);
      } else {
        logger.warn("Receive ChunkFetchFailure, errorMsg {}", resp.errorString);
        info.callback.onFailure(
            resp.streamChunkSlice.chunkIndex,
            new ChunkFetchFailureException(
                "Failure while fetching " + resp.streamChunkSlice + ": " + resp.errorString));
      }
    } else if (message instanceof RpcResponse) {
      RpcResponse resp = (RpcResponse) message;
      PushRequestInfo info = removePushRequest(resp.requestId);
      if (info == null) {
        RpcResponseCallback listener = removeRpcRequest(resp.requestId);
        if (listener == null) {
          logger.warn(
              "Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
              resp.requestId,
              NettyUtils.getRemoteAddress(channel),
              resp.body().size());
          resp.body().release();
        } else {
          try {
            listener.onSuccess(resp.body().nioByteBuffer());
          } finally {
            resp.body().release();
          }
        }
      } else {
        try {
          info.callback.onSuccess(resp.body().nioByteBuffer());
        } finally {
          resp.body().release();
        }
      }
    } else if (message instanceof RpcFailure) {
      RpcFailure resp = (RpcFailure) message;
      PushRequestInfo info = removePushRequest(resp.requestId);
      if (info == null) {
        RpcResponseCallback listener = removeRpcRequest(resp.requestId);
        if (listener == null) {
          logger.warn(
              "Ignoring response for RPC {} from {} ({}) since it is not outstanding",
              resp.requestId,
              NettyUtils.getRemoteAddress(channel),
              resp.errorString);
        } else {
          listener.onFailure(new IOException(resp.errorString));
        }
      } else {
        info.callback.onFailure(new CelebornIOException(resp.errorString));
      }
    } else {
      throw new IllegalStateException("Unknown response type: " + message.type());
    }
  }

  /** Returns total number of outstanding requests (fetch requests + rpcs) */
  public int numOutstandingRequests() {
    return outstandingFetches.size() + outstandingRpcs.size() + outstandingPushes.size();
  }

  /** Returns the time in nanoseconds of when the last request was sent out. */
  public long getTimeOfLastRequestNs() {
    return timeOfLastRequestNs.get();
  }

  /** Updates the time of the last request to the current system time. */
  public void updateTimeOfLastRequest() {
    timeOfLastRequestNs.set(System.nanoTime());
  }

  public void handleRpcFailure(long rpcRequestId, String errorMsg, Throwable cause) {
    RpcResponseCallback callback = removeRpcRequest(rpcRequestId);
    if (callback != null) {
      callback.onFailure(new CelebornIOException(errorMsg, cause));
    } else {
      logger.warn(
          "RpcResponseCallback {} not found/already addressed when listener handles rpc request failure",
          rpcRequestId);
    }
  }

  public void handlePushFailure(long pushRequestId, String errorMsg, Throwable cause) {
    PushRequestInfo info = removePushRequest(pushRequestId);
    if (info != null) {
      RpcResponseCallback callback = info.callback;
      if (callback != null) {
        callback.onFailure(new CelebornIOException(errorMsg, cause));
      } else {
        logger.warn(
            "PushRequestInfo {} callback is null when handle push request failure", pushRequestId);
      }
    } else {
      logger.warn(
          "PushRequestInfo {} not found/already addressed when listener handles push request failure",
          pushRequestId);
    }
  }

  public void handleFetchFailure(
      StreamChunkSlice streamChunkSlice, String errorMsg, Throwable cause) {
    FetchRequestInfo info = removeFetchRequest(streamChunkSlice);
    if (info != null) {
      ChunkReceivedCallback callback = info.callback;
      if (callback != null) {
        callback.onFailure(streamChunkSlice.chunkIndex, new IOException(errorMsg, cause));
      } else {
        logger.warn(
            "FetchRequestInfo ({}) callback is null when listener handles fetch request failure",
            streamChunkSlice);
      }
    } else {
      logger.warn(
          "FetchRequestInfo ({}) not found/already addressed when listener handles fetch request failure",
          streamChunkSlice);
    }
  }
}
