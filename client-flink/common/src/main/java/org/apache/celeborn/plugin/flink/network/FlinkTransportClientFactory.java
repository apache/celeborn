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

package org.apache.celeborn.plugin.flink.network;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.util.JavaUtils;

public class FlinkTransportClientFactory extends TransportClientFactory {

  public static final Logger logger = LoggerFactory.getLogger(FlinkTransportClientFactory.class);

  private ConcurrentHashMap<Long, Supplier<ByteBuf>> bufferSuppliers;

  private int bufferSizeBytes;

  public FlinkTransportClientFactory(
      TransportContext context, List<TransportClientBootstrap> bootstraps, int bufferSizeBytes) {
    super(context, bootstraps);
    bufferSuppliers = JavaUtils.newConcurrentHashMap();
    this.allocator = new UnpooledByteBufAllocator(true);
    this.bufferSizeBytes = bufferSizeBytes;
  }

  public TransportClient createClientWithRetry(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    return retryCreateClient(
        remoteHost,
        remotePort,
        -1,
        () -> new TransportFrameDecoderWithBufferSupplier(bufferSuppliers, bufferSizeBytes));
  }

  public void registerSupplier(long streamId, Supplier<ByteBuf> supplier) {
    bufferSuppliers.put(streamId, supplier);
  }

  public void unregisterSupplier(long streamId) {
    bufferSuppliers.remove(streamId);
  }
}
