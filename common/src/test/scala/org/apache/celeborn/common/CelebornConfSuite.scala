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

package org.apache.celeborn.common

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.internal.config.ConfigEntry
import org.apache.celeborn.common.network.util.IOMode
import org.apache.celeborn.common.protocol.StorageInfo

class CelebornConfSuite extends CelebornFunSuite {

  test("celeborn.master.endpoints support multi nodes") {
    val conf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, "localhost1:9097,localhost2:9097")
    val masterEndpoints = conf.masterEndpoints
    assert(masterEndpoints.length == 2)
    assert(masterEndpoints(0) == "localhost1:9097")
    assert(masterEndpoints(1) == "localhost2:9097")
  }

  test("storage test") {
    val conf = new CelebornConf()
    val defaultMaxUsableSpace = 1024L * 1024 * 1024 * 1024 * 1024
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, "/mnt/disk1")
    val workerBaseDirs = conf.workerBaseDirs
    assert(workerBaseDirs.size == 1)
    assert(workerBaseDirs.head._3 == 16)
    assert(workerBaseDirs.head._2 == defaultMaxUsableSpace)
  }

  test("storage test2") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, "/mnt/disk1:disktype=SSD:capacity=10g")
    val workerBaseDirs = conf.workerBaseDirs
    assert(workerBaseDirs.size == 1)
    assert(workerBaseDirs.head._3 == 16)
    assert(workerBaseDirs.head._2 == 10 * 1024 * 1024 * 1024L)
  }

  test("storage test3") {
    val conf = new CelebornConf()
    conf.set(
      CelebornConf.WORKER_STORAGE_DIRS.key,
      "/mnt/disk1:disktype=SSD:capacity=10g:flushthread=3")
    val workerBaseDirs = conf.workerBaseDirs
    assert(workerBaseDirs.size == 1)
    assert(workerBaseDirs.head._3 == 3)
    assert(workerBaseDirs.head._2 == 10 * 1024 * 1024 * 1024L)
  }

  test("storage test4") {
    val conf = new CelebornConf()
    conf.set(
      CelebornConf.WORKER_STORAGE_DIRS.key,
      "/mnt/disk1:disktype=SSD:capacity=10g:flushthread=3," +
        "/mnt/disk2:disktype=HDD:capacity=15g:flushthread=7")
    val workerBaseDirs = conf.workerBaseDirs
    assert(workerBaseDirs.size == 2)
    assert(workerBaseDirs.head._1 == "/mnt/disk1")
    assert(workerBaseDirs.head._3 == 3)
    assert(workerBaseDirs.head._2 == 10 * 1024 * 1024 * 1024L)

    assert(workerBaseDirs(1)._1 == "/mnt/disk2")
    assert(workerBaseDirs(1)._3 == 7)
    assert(workerBaseDirs(1)._2 == 15 * 1024 * 1024 * 1024L)
  }

  test("storage test5") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, "/mnt/disk1")
    val workerBaseDirs = conf.workerBaseDirs
    assert(workerBaseDirs.head._3 == 16)
  }

  test("storage test6") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_FLUSHER_THREADS.key, "4")
      .set(CelebornConf.WORKER_STORAGE_DIRS.key, "/mnt/disk1")
    val workerBaseDirs = conf.workerBaseDirs
    assert(workerBaseDirs.head._3 == 4)
  }

  test("storage test7") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_FLUSHER_THREADS.key, "4")
      .set(CelebornConf.WORKER_STORAGE_DIRS.key, "/mnt/disk1:flushthread=8")
    val workerBaseDirs = conf.workerBaseDirs
    assert(workerBaseDirs.head._3 == 8)
  }

  test("storage test8") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_FLUSHER_THREADS.key, "4")
      .set(CelebornConf.WORKER_STORAGE_DIRS.key, "/mnt/disk1:disktype=SSD")
    val workerBaseDirs = conf.workerBaseDirs
    assert(workerBaseDirs.head._3 == 16)
  }

  test("storage test9") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_FLUSHER_THREADS.key, "4")
      .set(CelebornConf.WORKER_STORAGE_DIRS.key, "/mnt/disk1:flushthread=9:disktype=HDD")
    val workerBaseDirs = conf.workerBaseDirs
    assert(workerBaseDirs.head._3 == 9)
  }

  test("CELEBORN-1095: Support configuration of fastest available XXHashFactory instance for checksum of Lz4Decompressor") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.SHUFFLE_DECOMPRESSION_LZ4_XXHASH_INSTANCE.key, "JNI")
    assert(conf.shuffleDecompressionLz4XXHashInstance.get == "JNI")
    conf.set(CelebornConf.SHUFFLE_DECOMPRESSION_LZ4_XXHASH_INSTANCE.key, "JAVASAFE")
    assert(conf.shuffleDecompressionLz4XXHashInstance.get == "JAVASAFE")
    conf.set(CelebornConf.SHUFFLE_DECOMPRESSION_LZ4_XXHASH_INSTANCE.key, "JAVAUNSAFE")
    assert(conf.shuffleDecompressionLz4XXHashInstance.get == "JAVAUNSAFE")
    val error = intercept[IllegalArgumentException] {
      conf.set(CelebornConf.SHUFFLE_DECOMPRESSION_LZ4_XXHASH_INSTANCE.key, "NATIVE")
      assert(conf.shuffleDecompressionLz4XXHashInstance.get == "NATIVE")
    }.getMessage
    assert(error.contains(
      s"The value of ${CelebornConf.SHUFFLE_DECOMPRESSION_LZ4_XXHASH_INSTANCE.key} should be one of JNI, JAVASAFE, JAVAUNSAFE, but was NATIVE"))
  }

  test("zstd level") {
    val conf = new CelebornConf()
    val error1 = intercept[IllegalArgumentException] {
      conf.set(CelebornConf.SHUFFLE_COMPRESSION_ZSTD_LEVEL.key, "-100")
      assert(conf.shuffleCompressionZstdCompressLevel == -100)
    }.getMessage
    assert(error1.contains(s"'-100' in ${SHUFFLE_COMPRESSION_ZSTD_LEVEL.key} is invalid. " +
      "Compression level for Zstd compression codec should be an integer between -5 and 22."))
    conf.set(CelebornConf.SHUFFLE_COMPRESSION_ZSTD_LEVEL.key, "-5")
    assert(conf.shuffleCompressionZstdCompressLevel == -5)
    conf.set(CelebornConf.SHUFFLE_COMPRESSION_ZSTD_LEVEL.key, "0")
    assert(conf.shuffleCompressionZstdCompressLevel == 0)
    conf.set(CelebornConf.SHUFFLE_COMPRESSION_ZSTD_LEVEL.key, "22")
    assert(conf.shuffleCompressionZstdCompressLevel == 22)
    val error2 = intercept[IllegalArgumentException] {
      conf.set(CelebornConf.SHUFFLE_COMPRESSION_ZSTD_LEVEL.key, "100")
      assert(conf.shuffleCompressionZstdCompressLevel == 100)
    }.getMessage
    assert(error2.contains(s"'100' in ${SHUFFLE_COMPRESSION_ZSTD_LEVEL.key} is invalid. " +
      "Compression level for Zstd compression codec should be an integer between -5 and 22."))
  }

  test("extract masterNodeIds") {
    val conf = new CelebornConf()
      .set("celeborn.master.ha.node.id", "1")
      .set("celeborn.master.ha.node.1.host", "clb-1")
      .set("celeborn.master.ha.node.2.host", "clb-1")
      .set("celeborn.master.ha.node.3.host", "clb-1")
    assert(conf.haMasterNodeIds.sorted === Array("1", "2", "3"))
  }

  test("CELEBORN-593: Test each RPC timeout value") {
    val conf = new CelebornConf()
      .set(CelebornConf.RPC_ASK_TIMEOUT, 1000L)
      .set(CelebornConf.NETWORK_TIMEOUT, 20000L)
      .set(CelebornConf.NETWORK_CONNECT_TIMEOUT, 2000L)

    assert(conf.rpcAskTimeout.duration.toMillis == 1000L)
    assert(conf.masterClientRpcAskTimeout.duration.toMillis == 1000L)
    assert(conf.clientRpcReserveSlotsRpcTimeout.duration.toMillis == 1000L)
    assert(conf.networkTimeout.duration.toMillis == 20000L)
    assert(conf.networkIoConnectionTimeoutMs("data") == 20000L)
    assert(conf.clientPushStageEndTimeout == 20000L)
    assert(conf.clientRpcRegisterShuffleAskTimeout.duration.toMillis == 1000L)
    assert(conf.clientRpcRequestPartitionLocationAskTimeout.duration.toMillis == 1000L)
    assert(conf.clientRpcGetReducerFileGroupAskTimeout.duration.toMillis == 1000L)
    assert(conf.networkConnectTimeout.duration.toMillis == 2000L)
    assert(conf.networkIoConnectTimeoutMs("data") == 2000L)
  }

  test("CELEBORN-601: Consolidate configsWithAlternatives with `ConfigBuilder.withAlternative`") {
    val conf = new CelebornConf()
      .set(CelebornConf.TEST_ALTERNATIVE.alternatives.head._1, "celeborn")

    assert(conf.testAlternative == "celeborn")
  }

  test("Test empty working dir") {
    val conf = new CelebornConf()
    conf.set("celeborn.storage.availableTypes", "HDFS")
    conf.set("celeborn.storage.hdfs.dir", "hdfs:///xxx")
    assert(conf.workerBaseDirs.isEmpty)

    conf.set("celeborn.storage.availableTypes", "SSD,HDD,HDFS")
    conf.set("celeborn.storage.hdfs.dir", "hdfs:///xxx")
    assert(conf.workerBaseDirs.isEmpty)

    conf.set("celeborn.storage.availableTypes", "SSD,HDD")
    assert(!conf.workerBaseDirs.isEmpty)

    conf.set("celeborn.storage.availableTypes", "S3")
    conf.set("celeborn.storage.s3.dir", "s3a:///xxx")
    assert(conf.workerBaseDirs.isEmpty)
  }

  test("Test commit file threads") {
    val conf = new CelebornConf()
    conf.set("celeborn.storage.availableTypes", "HDFS")
    conf.set("celeborn.storage.hdfs.dir", "hdfs:///xxx")
    assert(conf.workerCommitThreads === 128)

    conf.set("celeborn.storage.availableTypes", "SSD,HDD")
    assert(conf.workerCommitThreads === 32)

    conf.set("celeborn.storage.availableTypes", "S3")
    conf.set("celeborn.storage.s3.dir", "s3a:///xxx")
    assert(conf.workerCommitThreads === 32)
  }

  test("Test commit buffer size") {
    val conf = new CelebornConf()
    conf.set("celeborn.storage.availableTypes", "S3")
    conf.set("celeborn.storage.s3.dir", "s3a:///xxx")
    assert(conf.workerS3FlusherBufferSize === 6291456)
  }

  test("Test available storage types") {
    val conf = new CelebornConf()

    assert(conf.availableStorageTypes == StorageInfo.LOCAL_DISK_MASK)

    conf.set("celeborn.storage.availableTypes", "HDD,MEMORY")
    assert(conf.availableStorageTypes == Integer.parseInt("11", 2))

    conf.set("celeborn.storage.availableTypes", "HDD,HDFS")
    assert(conf.availableStorageTypes == (StorageInfo.HDFS_MASK | StorageInfo.LOCAL_DISK_MASK))

    conf.set("celeborn.storage.availableTypes", "HDFS")
    assert(conf.availableStorageTypes == StorageInfo.HDFS_MASK)

    conf.set("celeborn.storage.availableTypes", "S3")
    assert(conf.availableStorageTypes == StorageInfo.S3_MASK)

    conf.set("celeborn.storage.availableTypes", "OSS")
    assert(conf.availableStorageTypes == StorageInfo.OSS_MASK)
  }

  test("Test role rpcDispatcherNumThreads") {
    val availableCores = 5
    val conf = new CelebornConf()
    assert(conf.rpcDispatcherNumThreads(availableCores, "shuffleclient") === 5)

    conf.set("celeborn.shuffleclient.rpc.dispatcher.threads", "1")
    assert(conf.rpcDispatcherNumThreads(availableCores, "shuffleclient") === 1)
    assert(conf.rpcDispatcherNumThreads(availableCores, "lifecyclemanager") === 5)

    conf.set("celeborn.rpc.dispatcher.threads", "2")
    assert(conf.rpcDispatcherNumThreads(availableCores, "lifecyclemanager") === 2)

    conf.unset("celeborn.rpc.dispatcher.threads")
    conf.set("celeborn.rpc.dispatcher.numThreads", "3")
    assert(conf.rpcDispatcherNumThreads(availableCores, "lifecyclemanager") === 3)
  }

  test("Test rpcDispatcherNumThreads") {
    val availableCores = 5
    val conf = new CelebornConf()
    assert(conf.rpcDispatcherNumThreads(availableCores) === 5)
  }

  // Transport conf tests

  private val transportTestNetworkIoMode = IOMode.EPOLL.name()
  private val transportTestNetworkIoPreferDirectBufs =
    !NETWORK_IO_PREFER_DIRECT_BUFS.defaultValue.get

  private val transportTestNetworkIoConnectTimeout = 50000
  private val transportTestNetworkIoConnectionTimeout = 1000
  private val transportTestNetworkIoNumConnectionsPerPeer =
    NETWORK_IO_NUM_CONNECTIONS_PER_PEER.defaultValue.get + 5
  private val transportTestNetworkIoBacklog = NETWORK_IO_BACKLOG.defaultValue.get + 5
  private val transportTestNetworkIoServerThreads = NETWORK_IO_SERVER_THREADS.defaultValue.get + 5
  private val transportTestNetworkIoClientThreads = NETWORK_IO_CLIENT_THREADS.defaultValue.get + 5
  private val transportTestNetworkIoReceiveBuffer = NETWORK_IO_RECEIVE_BUFFER.defaultValue.get + 5
  private val transportTestNetworkIoSendBuffer = NETWORK_IO_SEND_BUFFER.defaultValue.get + 5
  private val transportTestNetworkIoMaxRetries = NETWORK_IO_MAX_RETRIES.defaultValue.get + 5
  private val transportTestNetworkIoRetryWait = NETWORK_IO_RETRY_WAIT.defaultValue.get + 5
  private val transportTestNetworkIoStorageMemoryMapThreshold =
    NETWORK_IO_STORAGE_MEMORY_MAP_THRESHOLD.defaultValue.get + 5
  private val transportTestNetworkIoLazyFd = !NETWORK_IO_LAZY_FD.defaultValue.get
  private val transportTestChannelHeartbeatInterval =
    CHANNEL_HEARTBEAT_INTERVAL.defaultValue.get + 5
  private val transportTestPushTimeoutCheckThreads = PUSH_TIMEOUT_CHECK_THREADS.defaultValue.get + 5
  private val transportTestPushTimeoutCheckInterval =
    PUSH_TIMEOUT_CHECK_INTERVAL.defaultValue.get + 5
  private val transportTestFetchTimeoutCheckThreads =
    FETCH_TIMEOUT_CHECK_THREADS.defaultValue.get + 5
  private val transportTestFetchTimeoutCheckInterval =
    FETCH_TIMEOUT_CHECK_INTERVAL.defaultValue.get + 5
  private val transportTestNetworkIoSaslTimeout = NETWORK_IO_SASL_TIMEOUT.defaultValue.get + 5

  private def setupCelebornConfForTransportTests(module: String): CelebornConf = {
    val conf = new CelebornConf()

    def moduleKey(config: ConfigEntry[_]): String = {
      config.key.replace("<module>", module)
    }

    conf.set(moduleKey(NETWORK_IO_MODE), transportTestNetworkIoMode)
    conf.set(
      moduleKey(NETWORK_IO_PREFER_DIRECT_BUFS),
      transportTestNetworkIoPreferDirectBufs.toString)
    conf.set(moduleKey(NETWORK_IO_CONNECT_TIMEOUT), transportTestNetworkIoConnectTimeout.toString)
    conf.set(
      moduleKey(NETWORK_IO_CONNECTION_TIMEOUT),
      transportTestNetworkIoConnectionTimeout.toString)
    conf.set(
      moduleKey(NETWORK_IO_NUM_CONNECTIONS_PER_PEER),
      transportTestNetworkIoNumConnectionsPerPeer.toString)
    conf.set(moduleKey(NETWORK_IO_BACKLOG), transportTestNetworkIoBacklog.toString)
    conf.set(moduleKey(NETWORK_IO_SERVER_THREADS), transportTestNetworkIoServerThreads.toString)
    conf.set(moduleKey(NETWORK_IO_CLIENT_THREADS), transportTestNetworkIoClientThreads.toString)
    conf.set(moduleKey(NETWORK_IO_RECEIVE_BUFFER), transportTestNetworkIoReceiveBuffer.toString)
    conf.set(moduleKey(NETWORK_IO_SEND_BUFFER), transportTestNetworkIoSendBuffer.toString)
    conf.set(moduleKey(NETWORK_IO_MAX_RETRIES), transportTestNetworkIoMaxRetries.toString)
    conf.set(moduleKey(NETWORK_IO_RETRY_WAIT), transportTestNetworkIoRetryWait.toString)
    conf.set(
      moduleKey(NETWORK_IO_STORAGE_MEMORY_MAP_THRESHOLD),
      transportTestNetworkIoStorageMemoryMapThreshold.toString)
    conf.set(moduleKey(NETWORK_IO_LAZY_FD), transportTestNetworkIoLazyFd.toString)
    conf.set(moduleKey(CHANNEL_HEARTBEAT_INTERVAL), transportTestChannelHeartbeatInterval.toString)
    conf.set(moduleKey(PUSH_TIMEOUT_CHECK_THREADS), transportTestPushTimeoutCheckThreads.toString)
    conf.set(moduleKey(PUSH_TIMEOUT_CHECK_INTERVAL), transportTestPushTimeoutCheckInterval.toString)
    conf.set(moduleKey(FETCH_TIMEOUT_CHECK_THREADS), transportTestFetchTimeoutCheckThreads.toString)
    conf.set(
      moduleKey(FETCH_TIMEOUT_CHECK_INTERVAL),
      transportTestFetchTimeoutCheckInterval.toString)
    conf.set(moduleKey(NETWORK_IO_SASL_TIMEOUT), transportTestNetworkIoSaslTimeout.toString)

    conf
  }

  private def validateDefaultTransportConfValue(conf: CelebornConf, module: String): Unit = {
    assert(transportTestNetworkIoMode == conf.networkIoMode(module))
    assert(transportTestNetworkIoPreferDirectBufs == conf.networkIoPreferDirectBufs(module))
    assert(transportTestNetworkIoConnectTimeout == conf.networkIoConnectTimeoutMs(module))
    assert(transportTestNetworkIoConnectionTimeout == conf.networkIoConnectionTimeoutMs(module))
    assert(
      transportTestNetworkIoNumConnectionsPerPeer == conf.networkIoNumConnectionsPerPeer(module))
    assert(transportTestNetworkIoBacklog == conf.networkIoBacklog(module))
    assert(transportTestNetworkIoServerThreads == conf.networkIoServerThreads(module))
    assert(transportTestNetworkIoClientThreads == conf.networkIoClientThreads(module))
    assert(transportTestNetworkIoReceiveBuffer == conf.networkIoReceiveBuf(module))
    assert(transportTestNetworkIoSendBuffer == conf.networkIoSendBuf(module))
    assert(transportTestNetworkIoMaxRetries == conf.networkIoMaxRetries(module))
    assert(transportTestNetworkIoRetryWait == conf.networkIoRetryWaitMs(module))
    assert(transportTestNetworkIoStorageMemoryMapThreshold == conf.networkIoMemoryMapBytes(module))
    assert(transportTestNetworkIoLazyFd == conf.networkIoLazyFileDescriptor(module))
    assert(transportTestChannelHeartbeatInterval == conf.channelHeartbeatInterval(module))
    assert(transportTestPushTimeoutCheckThreads == conf.pushDataTimeoutCheckerThreads(module))
    assert(transportTestPushTimeoutCheckInterval == conf.pushDataTimeoutCheckInterval(module))
    assert(transportTestFetchTimeoutCheckThreads == conf.fetchDataTimeoutCheckerThreads(module))
    assert(transportTestFetchTimeoutCheckInterval == conf.fetchDataTimeoutCheckInterval(module))
    assert(transportTestNetworkIoSaslTimeout == conf.networkIoSaslTimoutMs(module))
  }

  test("Basic fetch module config") {
    val conf = setupCelebornConfForTransportTests("test")
    validateDefaultTransportConfValue(conf, "test")
  }

  test("Fallback to parent module's config for transport conf when not defined for module") {
    val conf = setupCelebornConfForTransportTests("test_parent_module")
    // set in parent, but should work in child
    validateDefaultTransportConfValue(conf, "test_child_module")
  }

  test("rpc_service and rpc_client should default to rpc if not configured") {
    val conf = setupCelebornConfForTransportTests("rpc")
    // set in rpc, so should work for specific rpc servers
    validateDefaultTransportConfValue(conf, "rpc_service")
    validateDefaultTransportConfValue(conf, "rpc_app")
  }

  test("Test fallback config works even with parent") {
    // Using NETWORK_IO_CONNECT_TIMEOUT since it has fallback to NETWORK_CONNECT_TIMEOUT

    val fallbackValue = 100001
    val parentValue = 100002
    val childValue = 100003

    val conf = new CelebornConf()
    conf.set("celeborn.test_child_module.io.connectTimeout", childValue.toString)
    conf.set("celeborn.test_parent_module.io.connectTimeout", parentValue.toString)
    conf.set("celeborn.network.connect.timeout", fallbackValue.toString)

    assert(conf.networkIoConnectTimeoutMs("test_child_module") == childValue)

    // remove child config, it should use parent value now
    conf.unset("celeborn.test_child_module.io.connectTimeout")
    assert(conf.networkIoConnectTimeoutMs("test_child_module") == parentValue)

    // now remove parent as well, it should go to fallback value
    conf.unset("celeborn.test_parent_module.io.connectTimeout")
    assert(conf.networkIoConnectTimeoutMs("test_child_module") == fallbackValue)
  }

  test("Test storage policy case 1") {
    val conf = new CelebornConf()

    conf.set("celeborn.worker.storage.storagePolicy.createFilePolicy", "MEMORY,S3")
    val createFilePolicy = conf.workerStoragePolicyCreateFilePolicy
    assert(List("MEMORY", "S3") == createFilePolicy.get)

    conf.set("celeborn.worker.storage.storagePolicy.createFilePolicy", "MEMORY,SSD")
    val createFilePolicy1 = conf.workerStoragePolicyCreateFilePolicy
    assert(List("MEMORY", "SSD") == createFilePolicy1.get)

    conf.set("celeborn.worker.storage.storagePolicy.createFilePolicy", "MEMORY,HDFS")
    val createFilePolicy2 = conf.workerStoragePolicyCreateFilePolicy
    assert(List("MEMORY", "HDFS") == createFilePolicy2.get)

    conf.unset("celeborn.worker.storage.storagePolicy.createFilePolicy")
    val createFilePolicy3 = conf.workerStoragePolicyCreateFilePolicy
    assert(List("MEMORY", "SSD", "HDD", "HDFS", "S3", "OSS") == createFilePolicy3.get)

    try {
      conf.set("celeborn.worker.storage.storagePolicy.createFilePolicy", "ABC")
      val createFilePolicy4 = conf.workerStoragePolicyCreateFilePolicy
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[IllegalArgumentException])
    }
  }

  test("Test storage policy case 2") {
    val conf = new CelebornConf()
    conf.set("celeborn.worker.storage.storagePolicy.evictPolicy", "MEMORY,SSD")
    val evictPolicy1 = conf.workerStoragePolicyEvictFilePolicy
    assert(Map("MEMORY" -> List("SSD")) == evictPolicy1.get)

    conf.set("celeborn.worker.storage.storagePolicy.evictPolicy", "MEMORY,SSD,HDFS|HDD,HDFS")
    val evictPolicy2 = conf.workerStoragePolicyEvictFilePolicy
    assert(Map("MEMORY" -> List("SSD", "HDFS"), "HDD" -> List("HDFS")) == evictPolicy2.get)

    conf.unset("celeborn.worker.storage.storagePolicy.evictPolicy")
    val evictPolicy3 = conf.workerStoragePolicyEvictFilePolicy
    assert(Map("MEMORY" -> List("SSD", "HDD", "HDFS", "S3", "OSS")) == evictPolicy3.get)

    try {
      conf.set("celeborn.worker.storage.storagePolicy.evictPolicy", "ABC")
      conf.workerStoragePolicyEvictFilePolicy
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[IllegalArgumentException])
    }
  }

  test("CELEBORN-1511: Test master endpoint resolver") {
    val conf = new CelebornConf()

    val validResolverClass = "org.apache.celeborn.common.client.StaticMasterEndpointResolver"
    val invalidResolverClass = "org.apache.celeborn.UnknownClass"

    conf.set(MASTER_ENDPOINTS_RESOLVER.key, validResolverClass)
    assert(conf.masterEndpointResolver == validResolverClass)

    try {
      conf.set(MASTER_ENDPOINTS_RESOLVER.key, invalidResolverClass)
      val _ = conf.masterEndpointResolver
    } catch {
      case e: Exception =>
        assert(e.isInstanceOf[IllegalArgumentException])
        assert(e.getMessage.contains("Resolver class was not found in the classpath."))
    }
  }

}
