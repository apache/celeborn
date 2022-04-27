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

package com.aliyun.emr.rss.service.deploy.worker;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.network.client.RpcResponseCallback;
import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.protocol.MergedData;
import com.aliyun.emr.rss.common.network.server.OneForOneStreamManager;
import com.aliyun.emr.rss.common.network.server.RpcHandler;
import com.aliyun.emr.rss.common.network.server.StreamManager;
import com.aliyun.emr.rss.common.network.util.TransportConf;

public final class PushDataRpcHandler extends RpcHandler {

  private static final Logger logger = LoggerFactory.getLogger(PushDataRpcHandler.class);

  private final TransportConf conf;
  private final PushHandler handler;
  private final OneForOneStreamManager streamManager;

  public PushDataRpcHandler(TransportConf conf, PushHandler handler) {
    this.conf = conf;
    this.handler = handler;
    streamManager = new OneForOneStreamManager();
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    throw new UnsupportedOperationException("PushDataRpcHandler");
  }

  @Override
  public void receivePushMergedData(
      TransportClient client, MergedData mergedData, RpcResponseCallback callback) {
    handler.handleMergedData(mergedData, callback);
  }

  @Override
  public boolean checkRegistered() {
    return ((Worker) handler).isRegistered();
  }

  @Override
  public void channelInactive(TransportClient client) {
    logger.debug("channel Inactive " + client.getSocketAddress());
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    logger.debug("exception caught " + cause + " " + client.getSocketAddress());
  }

  @Override
  public StreamManager getStreamManager() {
    return streamManager;
  }
}
