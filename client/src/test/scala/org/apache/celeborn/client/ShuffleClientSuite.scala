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

package org.apache.celeborn.client

import java.io.IOException
import java.net.InetAddress
import java.net.UnknownHostException
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import io.netty.channel.ChannelFuture
import org.apache.commons.lang3.RandomStringUtils
import org.junit.Test
import org.mockito.Matchers.any
import org.mockito.Mockito.mock
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.apache.celeborn.client.compress.Compressor
import org.apache.celeborn.client.compress.Compressor.CompressionCodec
import org.apache.celeborn.common.RssConf
import org.apache.celeborn.common.network.client.{RpcResponseCallback, TransportClient, TransportClientFactory}
import org.apache.celeborn.common.network.protocol.{PushData, PushMergedData}
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.PbRegisterShuffleResponse
import org.apache.celeborn.common.protocol.message.ControlMessages.{RegisterShuffleResponse, _}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcEndpointRef
import org.mockito.stubbing.Answer

class ShuffleClientSuite {
  private var shuffleClient: ShuffleClientImpl = null
  private val endpointRef: RpcEndpointRef = mock(classOf[RpcEndpointRef])
  private val clientFactory: TransportClientFactory = mock(classOf[TransportClientFactory])
  private val client: TransportClient = mock(classOf[TransportClient])

  private val TEST_APPLICATION_ID: String = "testapp1"
  private val TEST_SHUFFLE_ID: Int = 1
  private val TEST_ATTEMPT_ID: Int = 0
  private val TEST_REDUCRE_ID: Int = 0

  private val MASTER_RPC_PORT: Int = 1234
  private val MASTER_PUSH_PORT: Int = 1235
  private val MASTER_FETCH_PORT: Int = 1236
  private val MASTER_REPLICATE_PORT: Int = 1237
  private val SLAVE_RPC_PORT: Int = 4321
  private val SLAVE_PUSH_PORT: Int = 4322
  private val SLAVE_FETCH_PORT: Int = 4323
  private val SLAVE_REPLICATE_PORT: Int = 4324
  private val masterLocation: PartitionLocation = new PartitionLocation(
    0,
    1,
    "localhost",
    MASTER_RPC_PORT,
    MASTER_PUSH_PORT,
    MASTER_FETCH_PORT,
    MASTER_REPLICATE_PORT,
    PartitionLocation.Mode.MASTER)
  private val slaveLocation: PartitionLocation = new PartitionLocation(
    0,
    1,
    "localhost",
    SLAVE_RPC_PORT,
    SLAVE_PUSH_PORT,
    SLAVE_FETCH_PORT,
    SLAVE_REPLICATE_PORT,
    PartitionLocation.Mode.SLAVE)

  private val TEST_BUF1: Array[Byte] = "hello world".getBytes(StandardCharsets.UTF_8)
  private val BATCH_HEADER_SIZE: Int = 4 * 4

  @Test
  @throws[IOException]
  @throws[InterruptedException]
  def testPushData(): Unit = {
    for (codec <- CompressionCodec.values) {
      val conf: RssConf = setupEnv(codec)
      val pushDataLen: Int = shuffleClient.pushData(
        TEST_APPLICATION_ID,
        TEST_SHUFFLE_ID,
        TEST_ATTEMPT_ID,
        TEST_ATTEMPT_ID,
        TEST_REDUCRE_ID,
        TEST_BUF1,
        0,
        TEST_BUF1.length,
        1,
        1)
      val compressor: Compressor = Compressor.getCompressor(conf)
      compressor.compress(TEST_BUF1, 0, TEST_BUF1.length)
      val compressedTotalSize: Int = compressor.getCompressedTotalSize
      assert((pushDataLen == compressedTotalSize + BATCH_HEADER_SIZE))
    }
  }

  @Test
  @throws[IOException]
  @throws[InterruptedException]
  def testMergeData(): Unit = {
    for (codec <- CompressionCodec.values) {
      val conf: RssConf = setupEnv(codec)
      val mergeSize: Int = shuffleClient.mergeData(
        TEST_APPLICATION_ID,
        TEST_SHUFFLE_ID,
        TEST_ATTEMPT_ID,
        TEST_ATTEMPT_ID,
        TEST_REDUCRE_ID,
        TEST_BUF1,
        0,
        TEST_BUF1.length,
        1,
        1)
      var compressor: Compressor = Compressor.getCompressor(conf)
      compressor.compress(TEST_BUF1, 0, TEST_BUF1.length)
      val compressedTotalSize: Int = compressor.getCompressedTotalSize
      shuffleClient.mergeData(
        TEST_APPLICATION_ID,
        TEST_SHUFFLE_ID,
        TEST_ATTEMPT_ID,
        TEST_ATTEMPT_ID,
        TEST_REDUCRE_ID,
        TEST_BUF1,
        0,
        TEST_BUF1.length,
        1,
        1)
      assert((mergeSize == compressedTotalSize + BATCH_HEADER_SIZE))
      val buf1k: Array[Byte] = RandomStringUtils.random(4000).getBytes(StandardCharsets.UTF_8)
      val largeMergeSize: Int = shuffleClient.mergeData(
        TEST_APPLICATION_ID,
        TEST_SHUFFLE_ID,
        TEST_ATTEMPT_ID,
        TEST_ATTEMPT_ID,
        TEST_REDUCRE_ID,
        buf1k,
        0,
        buf1k.length,
        1,
        1)
      compressor = Compressor.getCompressor(conf)
      compressor.compress(buf1k, 0, buf1k.length)
      val compressedTotalSize1: Int = compressor.getCompressedTotalSize
      assert((largeMergeSize == compressedTotalSize1 + BATCH_HEADER_SIZE))
    }
  }

  private def getLocalHost: String = {
    var ia: InetAddress = null
    if (ia == null) {
      try ia = InetAddress.getLocalHost
      catch {
        case e: UnknownHostException =>
          return null
      }
    }
    ia.getHostName
  }

  @throws[IOException]
  @throws[InterruptedException]
  private def setupEnv(codec: Compressor.CompressionCodec): RssConf = {
    val conf: RssConf = new RssConf
    conf.set("rss.client.compression.codec", codec.name)
    conf.set("rss.pushdata.retry.thread.num", "1")
    conf.set("rss.push.data.buffer.size", "1K")
    shuffleClient = new ShuffleClientImpl(conf, new UserIdentifier("mock", "mock"))
    masterLocation.setPeer(slaveLocation)
    when(endpointRef.askSync[PbRegisterShuffleResponse](RegisterShuffle(
      TEST_APPLICATION_ID,
      TEST_SHUFFLE_ID,
      1,
      1)))
      .thenAnswer(new Answer[PbRegisterShuffleResponse] {
        override def answer(invocationOnMock: InvocationOnMock): PbRegisterShuffleResponse =
          RegisterShuffleResponse(StatusCode.SUCCESS, Array[PartitionLocation](masterLocation))
      })
    shuffleClient.setupMetaServiceRef(endpointRef)
    val mockedFuture: ChannelFuture = mock(classOf[ChannelFuture])
    when(mockedFuture.isVoid).thenReturn(false)
    when(mockedFuture.isSuccess).thenReturn(true)
    when(mockedFuture.isCancellable).thenReturn(false)
    when(mockedFuture.await(any[Long](classOf[Long]))).thenReturn(true)
    when(
      mockedFuture.await(any[Long](classOf[Long]), any[TimeUnit](classOf[TimeUnit]))).thenReturn(
      true)
    when(mockedFuture.awaitUninterruptibly(any[Long](classOf[Long]))).thenReturn(true)
    when(mockedFuture.awaitUninterruptibly(
      any[Long](classOf[Long]),
      any[TimeUnit](classOf[TimeUnit]))).thenReturn(true)
    when(mockedFuture.cancel(any[Boolean](classOf[Boolean]))).thenReturn(false)
    when(mockedFuture.isCancelled).thenReturn(false)
    when(mockedFuture.isDone).thenReturn(true)
    when(client.pushData(any(classOf[PushData]), any(classOf[RpcResponseCallback])))
      .thenAnswer(new Answer[ChannelFuture] {
        override def answer(invocationOnMock: InvocationOnMock): ChannelFuture = mockedFuture
      })
    when(clientFactory.createClient(
      masterLocation.getHost,
      masterLocation.getPushPort,
      TEST_REDUCRE_ID))
      .thenAnswer(new Answer[TransportClient] {
        override def answer(invocationOnMock: InvocationOnMock): TransportClient = client
      })
    when(
      client.pushMergedData(
        any(classOf[PushMergedData]),
        any(classOf[RpcResponseCallback])))
      .thenAnswer(new Answer[ChannelFuture] {
        override def answer(invocationOnMock: InvocationOnMock): ChannelFuture = mockedFuture
      })
    when(clientFactory.createClient(masterLocation.getHost, masterLocation.getPushPort))
      .thenAnswer(
        new Answer[TransportClient] {
          override def answer(invocationOnMock: InvocationOnMock): TransportClient = client
        })
    shuffleClient.dataClientFactory = clientFactory
    conf
  }
}
