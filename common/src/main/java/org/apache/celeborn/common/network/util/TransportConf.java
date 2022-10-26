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

package org.apache.celeborn.common.network.util;

import com.google.common.primitives.Ints;

import org.apache.celeborn.common.CelebornConf;

/** A central location that tracks all the settings we expose to users. */
public class TransportConf {

  private final ConfigProvider conf;

  private final String module;

  public TransportConf(String module, ConfigProvider conf) {
    this.module = module;
    this.conf = conf;
  }

  public int getInt(String name, int defaultValue) {
    return conf.getInt(name, defaultValue);
  }

  public String get(String name, String defaultValue) {
    return conf.get(name, defaultValue);
  }

  public String getModuleName() {
    return module;
  }

  /** IO mode: nio or epoll */
  public String ioMode() {
    String key = CelebornConf.NETWORK_IO_MODE().key().replace("<module>", module).toUpperCase();
    String defaultValue = CelebornConf.NETWORK_IO_MODE().defaultValue().get();
    return conf.get(key, defaultValue);
  }

  /** If true, we will prefer allocating off-heap byte buffers within Netty. */
  public boolean preferDirectBufs() {
    String key = CelebornConf.NETWORK_IO_PREFER_DIRECT_BUFS().key().replace("<module>", module);
    boolean defaultValue =
        (boolean) CelebornConf.NETWORK_IO_PREFER_DIRECT_BUFS().defaultValue().get();
    return conf.getBoolean(key, defaultValue);
  }

  /** Connect timeout in milliseconds. Default 10 secs. */
  public int connectTimeoutMs() {
    String key = CelebornConf.NETWORK_IO_CONNECT_TIMEOUT().key().replace("<module>", module);
    long defaultValue = (long) CelebornConf.NETWORK_CONNECT_TIMEOUT().defaultValue().get();
    return (int) conf.getLong(key, defaultValue);
  }

  /** Connection active timeout in milliseconds. Default 240 secs. */
  public int connectionTimeoutMs() {
    String key = CelebornConf.NETWORK_IO_CONNECTION_TIMEOUT().key().replace("<module>", module);
    long defaultValue = (long) CelebornConf.NETWORK_TIMEOUT().defaultValue().get();
    return (int) conf.getLong(key, defaultValue);
  }

  /** Number of concurrent connections between two nodes for fetching data. */
  public int numConnectionsPerPeer() {
    String key =
        CelebornConf.NETWORK_IO_NUM_CONNECTIONS_PER_PEER().key().replace("<module>", module);
    int defaultValue =
        (int) CelebornConf.NETWORK_IO_NUM_CONNECTIONS_PER_PEER().defaultValue().get();
    return conf.getInt(key, defaultValue);
  }

  /** Requested maximum length of the queue of incoming connections. Default -1 for no backlog. */
  public int backLog() {
    String key = CelebornConf.NETWORK_IO_BACKLOG().key().replace("<module>", module);
    int defaultValue = (int) CelebornConf.NETWORK_IO_BACKLOG().defaultValue().get();
    return conf.getInt(key, defaultValue);
  }

  /** Number of threads used in the server thread pool. Default to 0, which is 2x#cores. */
  public int serverThreads() {
    String key = CelebornConf.NETWORK_IO_SERVER_THREADS().key().replace("<module>", module);
    int defaultValue = (int) CelebornConf.NETWORK_IO_SERVER_THREADS().defaultValue().get();
    return conf.getInt(key, defaultValue);
  }

  /** Number of threads used in the client thread pool. Default to 0, which is 2x#cores. */
  public int clientThreads() {
    String key = CelebornConf.NETWORK_IO_CLIENT_THREADS().key().replace("<module>", module);
    int defaultValue = (int) CelebornConf.NETWORK_IO_CLIENT_THREADS().defaultValue().get();
    return conf.getInt(key, defaultValue);
  }

  /**
   * Receive buffer size (SO_RCVBUF). Note: the optimal size for receive buffer and send buffer
   * should be latency * network_bandwidth. Assuming latency = 1ms, network_bandwidth = 10Gbps
   * buffer size should be ~ 1.25MB
   */
  public int receiveBuf() {
    String key = CelebornConf.NETWORK_IO_RECEIVE_BUFFER().key().replace("<module>", module);
    long defaultValue = (long) CelebornConf.NETWORK_IO_RECEIVE_BUFFER().defaultValue().get();
    return (int) conf.getLong(key, defaultValue);
  }

  /** Send buffer size (SO_SNDBUF). */
  public int sendBuf() {
    String key = CelebornConf.NETWORK_IO_SEND_BUFFER().key().replace("<module>", module);
    long defaultValue = (long) CelebornConf.NETWORK_IO_SEND_BUFFER().defaultValue().get();
    return (int) conf.getLong(key, defaultValue);
  }

  /**
   * Max number of times we will try IO exceptions (such as connection timeouts) per request. If set
   * to 0, we will not do any retries.
   */
  public int maxIORetries() {
    String key = CelebornConf.NETWORK_IO_MAX_RETRIES().key().replace("<module>", module);
    int defaultValue = (int) CelebornConf.NETWORK_IO_MAX_RETRIES().defaultValue().get();
    return conf.getInt(key, defaultValue);
  }

  /**
   * Time (in milliseconds) that we will wait in order to perform a retry after an IOException. Only
   * relevant if maxIORetries &gt; 0.
   */
  public int ioRetryWaitTimeMs() {
    String key = CelebornConf.NETWORK_IO_RETRY_WAIT().key().replace("<module>", module);
    long defaultValue = (long) CelebornConf.NETWORK_IO_RETRY_WAIT().defaultValue().get();
    return (int) conf.getLong(key, defaultValue);
  }

  /**
   * Minimum size of a block that we should start using memory map rather than reading in through
   * normal IO operations. This prevents Spark from memory mapping very small blocks. In general,
   * memory mapping has high overhead for blocks close to or below the page size of the OS.
   */
  public int memoryMapBytes() {
    String key = CelebornConf.STORAGE_MEMORY_MAP_THRESHOLD().key();
    long defaultValue = (long) CelebornConf.STORAGE_MEMORY_MAP_THRESHOLD().defaultValue().get();
    return Ints.checkedCast(conf.getLong(key, defaultValue));
  }

  /**
   * Whether to initialize FileDescriptor lazily or not. If true, file descriptors are created only
   * when data is going to be transferred. This can reduce the number of open files.
   */
  public boolean lazyFileDescriptor() {
    String key = CelebornConf.NETWORK_IO_LAZY_FD().key().replace("<module>", module);
    boolean defaultValue = (boolean) CelebornConf.NETWORK_IO_LAZY_FD().defaultValue().get();
    return conf.getBoolean(key, defaultValue);
  }

  /**
   * Whether to track Netty memory detailed metrics. If true, the detailed metrics of Netty
   * PoolByteBufAllocator will be gotten, otherwise only general memory usage will be tracked.
   */
  public boolean verboseMetrics() {
    String key = CelebornConf.NETWORK_VERBOSE_METRICS().key().replace("<module>", module);
    boolean defaultValue = (boolean) CelebornConf.NETWORK_VERBOSE_METRICS().defaultValue().get();
    return conf.getBoolean(key, defaultValue);
  }

  /**
   * The max number of chunks allowed to be transferred at the same time on shuffle service. Note
   * that new incoming connections will be closed when the max number is hit. The client will retry
   * according to the shuffle retry configs (see `celeborn.shuffle.io.maxRetries` and
   * `celeborn.shuffle.io.retryWait`), if those limits are reached the task will fail with fetch
   * failure.
   */
  public long maxChunksBeingTransferred() {
    String key = CelebornConf.MAX_CHUNKS_BEING_TRANSFERRED().key();
    long defaultValue = (long) CelebornConf.MAX_CHUNKS_BEING_TRANSFERRED().defaultValue().get();
    return conf.getLong(key, defaultValue);
  }

  public String decoderMode() {
    String key = CelebornConf.NETWORK_IO_DECODER_MODE().key().replace("<module>", module);
    String defaultValue = CelebornConf.NETWORK_IO_DECODER_MODE().defaultValue().get();
    return conf.get(key, defaultValue);
  }
}
