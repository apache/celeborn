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

package org.apache.celeborn.common.network.security;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.Channel;

import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServerBootstrap;
import org.apache.celeborn.common.network.util.TransportConf;

/** Per-instance close counts for failed owner construction tests. */
public class StartupCountingBootstrap
    implements TransportClientBootstrap, TransportServerBootstrap {
  private static final AtomicInteger NEXT_ID = new AtomicInteger();
  private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, AtomicInteger>> COUNTS =
      new ConcurrentHashMap<>();
  private final AtomicInteger closeCount = new AtomicInteger();

  public StartupCountingBootstrap(TransportConf conf) {
    COUNTS
        .computeIfAbsent(conf.getModuleName(), ignored -> new ConcurrentHashMap<>())
        .put(NEXT_ID.incrementAndGet(), closeCount);
  }

  public static void reset() {
    COUNTS.clear();
  }

  public static Map<Integer, Integer> closeCounts(String module) {
    Map<Integer, Integer> snapshot = new HashMap<>();
    Map<Integer, AtomicInteger> counts = COUNTS.get(module);
    if (counts != null) {
      counts.forEach((id, count) -> snapshot.put(id, count.get()));
    }
    return snapshot;
  }

  @Override
  public void doBootstrap(TransportClient client) {}

  @Override
  public BaseMessageHandler doBootstrap(Channel channel, BaseMessageHandler handler) {
    return handler;
  }

  @Override
  public void close() {
    closeCount.incrementAndGet();
  }
}
