package com.aliyun.emr.rss.service.deploy.cluster

import com.aliyun.emr.rss.client.ShuffleClientImpl

import java.nio.charset.StandardCharsets
import org.apache.commons.lang3.RandomStringUtils
import org.junit.Test
import com.aliyun.emr.rss.client.write.LifecycleManager
import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.service.deploy.MiniClusterFeature

class ClusterReviveTest extends MiniClusterFeature{
  @Test
  def testWorkerLost(): Unit ={
    val (worker1, workerRpcEnv1, worker2, workerRpcEnv2, worker3, workerRpcEnv3, worker4,
    workerRpcEnv4, workerRpcEnv5, worker5) = setUpMiniCluster(Map("rss.worker.timeout" -> "10s"),
      Map("rss.worker.flush.queue.capacity" -> "4", "rss.worker.timeout" -> "10s"))

    val APP1 = "APP-1"

    val clientConf = new RssConf()
    clientConf.set("rss.push.data.replicate", "true")
    clientConf.set("rss.push.data.buffer.size", "256K")
    val metaSystem = new LifecycleManager(APP1, clientConf)
    val shuffleClient = new ShuffleClientImpl(clientConf)
    shuffleClient.setupMetaServiceRef(metaSystem.self)

    val STR1 = RandomStringUtils.random(1024)
    val DATA1 = STR1.getBytes(StandardCharsets.UTF_8)
    val OFFSET1 = 0
    val LENGTH1 = DATA1.length

    val dataSize1 = shuffleClient.pushData(APP1, 1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 3, 3)
    logInfo(s"push data data size ${dataSize1}")
    shuffleClient.pushData(APP1, 1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 3, 3)

    shuffleClient.pushData(APP1, 1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 1, 0, 1, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 2, 0, 2, DATA1, OFFSET1, LENGTH1, 3, 3)

    shuffleClient.pushData(APP1, 1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 1, 0, 1, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 2, 0, 2, DATA1, OFFSET1, LENGTH1, 3, 3)

    shuffleClient.pushData(APP1, 1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 1, 0, 1, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 2, 0, 2, DATA1, OFFSET1, LENGTH1, 3, 3)

    worker2.rpcEnv.shutdown()
    worker2.stop()
    Thread.sleep(10000L)

    shuffleClient.pushData(APP1, 1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 1, 0, 1, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 2, 0, 2, DATA1, OFFSET1, LENGTH1, 3, 3)

    shuffleClient.pushData(APP1, 1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 1, 0, 1, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 2, 0, 2, DATA1, OFFSET1, LENGTH1, 3, 3)

    shuffleClient.pushData(APP1, 1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 1, 0, 1, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 2, 0, 2, DATA1, OFFSET1, LENGTH1, 3, 3)

    shuffleClient.pushData(APP1, 1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 1, 0, 1, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 2, 0, 2, DATA1, OFFSET1, LENGTH1, 3, 3)

    Thread.sleep(5000L)

    shuffleClient.pushData(APP1, 1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 1, 0, 1, DATA1, OFFSET1, LENGTH1, 3, 3)
    shuffleClient.pushData(APP1, 1, 2, 0, 2, DATA1, OFFSET1, LENGTH1, 3, 3)

    shuffleClient.mapperEnd(APP1, 1, 0, 1, 3)
    shuffleClient.mapperEnd(APP1, 1, 1, 1, 3)
    shuffleClient.mapperEnd(APP1, 1, 2, 1, 3)

    Thread.sleep(2000L)
    shuffleClient.unregisterShuffle(APP1, 1, true)
    Thread.sleep(2000L)
  }

}
