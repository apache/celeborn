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
import java.nio.ByteBuffer
import java.util
import java.util._
import java.util.concurrent._
import java.util.function.Function

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelFuture

import org.apache.celeborn.client.ShuffleClientImpl._
import org.apache.celeborn.client.compress.Compressor
import org.apache.celeborn.client.read.RssInputStream
import org.apache.celeborn.client.write.DataBatches
import org.apache.celeborn.client.write.PushState
import org.apache.celeborn.common.RssConf
import org.apache.celeborn.common.haclient.RssHARetryClient
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.network.TransportContext
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer
import org.apache.celeborn.common.network.client.RpcResponseCallback
import org.apache.celeborn.common.network.client.TransportClient
import org.apache.celeborn.common.network.client.TransportClientFactory
import org.apache.celeborn.common.network.protocol.PushData
import org.apache.celeborn.common.network.protocol.PushMergedData
import org.apache.celeborn.common.network.server.BaseMessageHandler
import org.apache.celeborn.common.network.util.TransportConf
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcAddress
import org.apache.celeborn.common.rpc.RpcEndpointRef
import org.apache.celeborn.common.rpc.RpcEnv
import org.apache.celeborn.common.unsafe.Platform
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.common.util.Utils

class ShuffleClientImpl extends ShuffleClient with Logging {
  private var conf: RssConf = null

  private var userIdentifier: UserIdentifier = null

  private var registerShuffleMaxRetries: Int = 0
  private var registerShuffleRetryWait: Long = 0L
  private var maxInFlight: Int = 0
  private var pushBufferSize: Int = 0

  private var rpcEnv: RpcEnv = null

  private var driverRssMetaService: RpcEndpointRef = null

  var dataClientFactory: TransportClientFactory = null

  private var ia: InetAddress = null

  // key: shuffleId, value: (partitionId, PartitionLocation)
  private val reducePartitionMap: Map[Integer, ConcurrentHashMap[Integer, PartitionLocation]] =
    new ConcurrentHashMap[Integer, ConcurrentHashMap[Integer, PartitionLocation]]

  private val mapperEndMap: ConcurrentHashMap[Integer, Set[String]] =
    new ConcurrentHashMap[Integer, Set[String]]

  // key: shuffleId-mapId-attemptId
  private val pushStates: Map[String, PushState] = new ConcurrentHashMap[String, PushState]

  private var pushDataRetryPool: ExecutorService = null

  private var partitionSplitPool: ExecutorService = null
  private val splitting: Map[Integer, Set[Integer]] = new ConcurrentHashMap[Integer, Set[Integer]]

  private[client] val compressorThreadLocal: ThreadLocal[Compressor] =
    new ThreadLocal[Compressor]() {
      override protected def initialValue: Compressor = {
        Compressor.getCompressor(conf)
      }
    }

  // key: shuffleId
  private val reduceFileGroupsMap: Map[Integer, ReduceFileGroups] =
    new ConcurrentHashMap[Integer, ReduceFileGroups]

  def this(conf: RssConf, userIdentifier: UserIdentifier) {
    this()
    this.conf = conf
    this.userIdentifier = userIdentifier
    registerShuffleMaxRetries = RssConf.registerShuffleMaxRetry(conf)
    registerShuffleRetryWait = RssConf.registerShuffleRetryWait(conf)
    maxInFlight = RssConf.pushDataMaxReqsInFlight(conf)
    pushBufferSize = RssConf.pushDataBufferSize(conf)
    // init rpc env and master endpointRef
    rpcEnv = RpcEnv.create("ShuffleClient", Utils.localHostName, 0, conf)
    val dataTransportConf: TransportConf = Utils.fromRssConf(
      conf,
      TransportModuleConstants.DATA_MODULE,
      conf.getInt("rss.data.io.threads", 8))
    val context: TransportContext =
      new TransportContext(dataTransportConf, new BaseMessageHandler, true)
    dataClientFactory = context.createClientFactory
    val retryThreadNum: Int = RssConf.pushDataRetryThreadNum(conf)
    pushDataRetryPool = ThreadUtils.newDaemonCachedThreadPool("Retry-Sender", retryThreadNum, 60)
    val splitPoolSize: Int = RssConf.clientSplitPoolSize(conf)
    partitionSplitPool = ThreadUtils.newDaemonCachedThreadPool("Shuffle-Split", splitPoolSize, 60)
  }

  private def submitRetryPushData(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      body: Array[Byte],
      batchId: Int,
      loc: PartitionLocation,
      callback: RpcResponseCallback,
      pushState: PushState,
      cause: StatusCode): Unit = {
    val partitionId: Int = loc.getId
    if (!revive(
        applicationId,
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        loc.getEpoch,
        loc,
        cause)) {
      callback.onFailure(new IOException("Revive Failed"))
    } else {
      if (mapperEnded(shuffleId, mapId, attemptId)) {
        logDebug(s"Retrying push data, but the mapper(map $mapId attempt $attemptId) has ended.")
        pushState.inFlightBatches.remove(batchId)
      } else {
        val newLoc: PartitionLocation = reducePartitionMap.get(shuffleId).get(partitionId)
        logInfo(s"Revive success, new location for reduce $partitionId is $newLoc.")
        try {
          val client: TransportClient =
            dataClientFactory.createClient(newLoc.getHost, newLoc.getPushPort, partitionId)
          val newBuffer: NettyManagedBuffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(body))
          val shuffleKey: String = Utils.makeShuffleKey(applicationId, shuffleId)
          val newPushData: PushData =
            new PushData(MASTER_MODE, shuffleKey, newLoc.getUniqueId, newBuffer)
          val future: ChannelFuture = client.pushData(newPushData, callback)
          pushState.addFuture(batchId, future)
        } catch {
          case ex: Exception =>
            logError(
              s"Exception raised while pushing data for shuffle $shuffleId map $mapId attempt $attemptId" + " batch batchId.",
              ex)
            callback.onFailure(ex)
        }
      }
    }
  }

  private def submitRetryPushMergedData(
      pushState: PushState,
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      batches: ArrayList[DataBatches.DataBatch],
      cause: StatusCode,
      oldGroupedBatchId: Integer): Unit = {
    val newDataBatchesMap: HashMap[String, DataBatches] = new HashMap[String, DataBatches]
    batches.asScala.foreach { batch =>
      val partitionId: Int = batch.loc.getId
      if (!revive(
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          batch.loc.getEpoch,
          batch.loc,
          cause)) {
        pushState.exception.compareAndSet(
          null,
          new IOException("Revive Failed in retry push merged data for location: " + batch.loc))
        return
      } else {
        if (mapperEnded(shuffleId, mapId, attemptId)) {
          logDebug(s"Retrying push data, but the mapper(map $mapId attempt $attemptId) has ended.")
        } else {
          val newLoc: PartitionLocation = reducePartitionMap.get(shuffleId).get(partitionId)
          logInfo(s"Revive success, new location for reduce $partitionId is $newLoc.")
          val newDataBatches: DataBatches = newDataBatchesMap.computeIfAbsent(
            genAddressPair(newLoc),
            new Function[String, DataBatches] {
              override def apply(v1: String): DataBatches = {
                new DataBatches
              }
            })
          newDataBatches.addDataBatch(newLoc, batch.batchId, batch.body)
        }
      }
    }
    newDataBatchesMap.entrySet().asScala.foreach { entry =>
      val addressPair: String = entry.getKey
      val newDataBatches: DataBatches = entry.getValue
      val tokens: Array[String] = addressPair.split("-")
      doPushMergedData(
        tokens(0),
        applicationId,
        shuffleId,
        mapId,
        attemptId,
        newDataBatches.requireBatches,
        pushState,
        true)
    }
    pushState.inFlightBatches.remove(oldGroupedBatchId)
  }

  private def genAddressPair(loc: PartitionLocation): String = {
    var addressPair: String = null
    if (loc.getPeer != null) {
      addressPair = loc.hostAndPushPort + "-" + loc.getPeer.hostAndPushPort
    } else {
      addressPair = loc.hostAndPushPort
    }
    addressPair
  }

  private def registerShuffle(
      appId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int): ConcurrentHashMap[Integer, PartitionLocation] = {
    var numRetries: Int = registerShuffleMaxRetries
    while (numRetries > 0) {
      try {
        val response: PbRegisterShuffleResponse =
          driverRssMetaService.askSync[PbRegisterShuffleResponse](RegisterShuffle(
            appId,
            shuffleId,
            numMappers,
            numPartitions))
        val respStatus: StatusCode = Utils.toStatusCode(response.getStatus)
        if (StatusCode.SUCCESS == respStatus) {
          val result: ConcurrentHashMap[Integer, PartitionLocation] =
            new ConcurrentHashMap[Integer, PartitionLocation]
          for (i <- 0 until response.getPartitionLocationsList.size) {
            val partitionLoc: PartitionLocation =
              PartitionLocation.fromPbPartitionLocation(response.getPartitionLocationsList.get(i))
            result.put(partitionLoc.getId, partitionLoc)
          }
          return result
        } else {
          if (StatusCode.SLOT_NOT_AVAILABLE == respStatus) {
            logWarning(s"LifecycleManager request slots return ${StatusCode.SLOT_NOT_AVAILABLE}, retry again, remain retry times ${numRetries - 1}")
          } else {
            logError(s"LifecycleManager request slots return ${StatusCode.FAILED}, retry again, remain retry times ${numRetries - 1}")
          }
        }
      } catch {
        case e: Exception =>
          logError(
            s"Exception raised while registering shuffle $shuffleId with $numMappers mapper and $numPartitions partitions.",
            e)
          return null
      }
      try {
        TimeUnit.SECONDS.sleep(registerShuffleRetryWait)
      } catch {
        case e: InterruptedException =>
          return null

      }
      numRetries -= 1
    }
    null
  }

  @throws[IOException]
  private def limitMaxInFlight(mapKey: String, pushState: PushState, limit: Int): Unit = {
    if (pushState.exception.get != null) {
      throw pushState.exception.get
    }
    val inFlightBatches: ConcurrentHashMap[Integer, PartitionLocation] = pushState.inFlightBatches
    val timeoutMs: Long = RssConf.limitInFlightTimeoutMs(conf)
    val delta: Long = RssConf.limitInFlightSleepDeltaMs(conf)
    var times: Long = timeoutMs / delta
    try {
      while (times > 0 && inFlightBatches.size > limit) {
        if (pushState.exception.get != null) {
          throw pushState.exception.get
        }
        Thread.sleep(delta)
        times -= 1
      }
    } catch {
      case e: InterruptedException =>
        pushState.exception.set(new IOException(e))
    }
    if (times <= 0) {
      logError(s"After waiting for $timeoutMs ms, there are still ${inFlightBatches.size} batches in flight for map $mapKey, which exceeds the limit $limit.")
      logError(s"Map: $mapKey in flight batches: $inFlightBatches")
      throw new IOException("wait timeout for task " + mapKey, pushState.exception.get)
    }
    if (pushState.exception.get != null) {
      throw pushState.exception.get
    }
  }

  private def waitRevivedLocation(
      map: ConcurrentHashMap[Integer, PartitionLocation],
      partitionId: Int,
      epoch: Int): Boolean = {
    var currentLocation: PartitionLocation = map.get(partitionId)
    if (currentLocation != null && currentLocation.getEpoch > epoch) {
      return true
    }
    val sleepTimeMs: Long = rand.nextInt(50)
    if (sleepTimeMs > 30) {
      try TimeUnit.MILLISECONDS.sleep(sleepTimeMs)
      catch {
        case e: InterruptedException =>
          logError("Wait revived location interrupted", e)
          Thread.currentThread.interrupt()
      }
    }
    currentLocation = map.get(partitionId)
    currentLocation != null && currentLocation.getEpoch > epoch
  }

  private def revive(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      epoch: Int,
      oldLocation: PartitionLocation,
      cause: StatusCode): Boolean = {
    val map: ConcurrentHashMap[Integer, PartitionLocation] = reducePartitionMap.get(shuffleId)
    if (waitRevivedLocation(map, partitionId, epoch)) {
      logDebug(
        s"Has already revived for shuffle $shuffleId map $mapId reduce $partitionId epoch $epoch, just return(Assume revive successfully).")
      return true
    }
    val mapKey: String = Utils.makeMapKey(shuffleId, mapId, attemptId)
    if (mapperEnded(shuffleId, mapId, attemptId)) {
      logDebug(
        s"The mapper(shuffle $shuffleId map $mapId) has already ended, just return(Assume revive successfully).")
      return true
    }
    try {
      val response: PbChangeLocationResponse =
        driverRssMetaService.askSync[PbChangeLocationResponse](Revive(
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          epoch,
          oldLocation,
          cause))
      // per partitionKey only serve single PartitionLocation in Client Cache.
      val respStatus: StatusCode = Utils.toStatusCode(response.getStatus)
      if (StatusCode.SUCCESS == respStatus) {
        map.put(partitionId, PartitionLocation.fromPbPartitionLocation(response.getLocation))
        true
      } else {
        if (StatusCode.MAP_ENDED == respStatus) {
          mapperEndMap.computeIfAbsent(
            shuffleId,
            new Function[Integer, Set[String]] {
              override def apply(t: Integer): util.Set[String] = {
                ConcurrentHashMap.newKeySet[String]
              }
            }).add(mapKey)
          true
        } else {
          false
        }
      }
    } catch {
      case e: Exception =>
        logError(
          s"Exception raised while reviving for shuffle $shuffleId reduce $partitionId epoch $epoch.",
          e)
        false
    }
  }

  @throws[IOException]
  def pushOrMergeData(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      data: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int,
      doPush: Boolean): Int = { // mapKey
    val mapKey: String = Utils.makeMapKey(shuffleId, mapId, attemptId)
    val shuffleKey: String = Utils.makeShuffleKey(applicationId, shuffleId)
    // return if shuffle stage already ended
    if (mapperEnded(shuffleId, mapId, attemptId)) {
      logDebug(s"The mapper(shuffle $shuffleId map $mapId attempt $attemptId) has already ended while pushing data.")
      val pushState: PushState = pushStates.get(mapKey)
      if (pushState != null) {
        pushState.cancelFutures()
      }
      return 0
    }
    // register shuffle if not registered
    val map: ConcurrentHashMap[Integer, PartitionLocation] = reducePartitionMap.computeIfAbsent(
      shuffleId,
      new Function[Integer, ConcurrentHashMap[Integer, PartitionLocation]] {
        override def apply(t: Integer): ConcurrentHashMap[Integer, PartitionLocation] = {
          registerShuffle(applicationId, shuffleId, numMappers, numPartitions)
        }
      })
    if (map == null) {
      throw new IOException("Register shuffle failed for shuffle " + shuffleKey)
    }
    // get location
    if (!(map.containsKey(partitionId)) && !(revive(
        applicationId,
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        0,
        null,
        StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE))) {
      throw new IOException(
        "Revive for shuffle " + shuffleKey + " partitionId " + partitionId + " failed.")
    }
    if (mapperEnded(shuffleId, mapId, attemptId)) {
      logDebug(
        s"The mapper(shuffle $shuffleId map $mapId attempt $attemptId) has already ended while" + " pushing data.")
      val pushState: PushState = pushStates.get(mapKey)
      if (pushState != null) {
        pushState.cancelFutures()
      }
      return 0
    }
    val loc: PartitionLocation = map.get(partitionId)
    if (loc == null) {
      throw new IOException(
        "Partition location for shuffle " + shuffleKey + " partitionId " + partitionId + " is NULL!")
    }
    val pushState: PushState = pushStates.computeIfAbsent(mapKey,
        new Function[String, PushState] {
          override def apply(t: String): PushState = {
            new PushState(conf)
          }
        })
    // increment batchId
    val nextBatchId: Int = pushState.batchId.addAndGet(1)
    // compress data
    val compressor: Compressor = compressorThreadLocal.get
    compressor.compress(data, offset, length)
    val compressedTotalSize: Int = compressor.getCompressedTotalSize
    val BATCH_HEADER_SIZE: Int = 4 * 4
    val body: Array[Byte] = new Array[Byte](BATCH_HEADER_SIZE + compressedTotalSize)
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET, mapId)
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 4, attemptId)
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 8, nextBatchId)
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 12, compressedTotalSize)
    System.arraycopy(
      compressor.getCompressedBuffer,
      0,
      body,
      BATCH_HEADER_SIZE,
      compressedTotalSize)
    if (doPush) {
      logDebug(
        s"Do push data for app $applicationId shuffle $shuffleId map $mapId attempt $attemptId reduce $partitionId batch $nextBatchId.")
      // check limit
      limitMaxInFlight(mapKey, pushState, maxInFlight)
      // add inFlight requests
      pushState.inFlightBatches.put(nextBatchId, loc)
      // build PushData request
      val buffer: NettyManagedBuffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(body))
      val pushData: PushData = new PushData(MASTER_MODE, shuffleKey, loc.getUniqueId, buffer)
      // build callback
      val callback: RpcResponseCallback = new RpcResponseCallback() {
        override def onSuccess(response: ByteBuffer): Unit = {
          pushState.inFlightBatches.remove(nextBatchId)
          if (response.remaining > 0 && response.get == StatusCode.STAGE_ENDED.getValue) {
            mapperEndMap.computeIfAbsent(
              shuffleId,
              new Function[Integer, Set[String]] {
                override def apply(t: Integer): util.Set[String] = {
                  ConcurrentHashMap.newKeySet[String]
                }
              }).add(mapKey)
          }
          pushState.removeFuture(nextBatchId)
          logDebug(s"Push data to ${loc.getHost}:${loc.getPushPort} success for map $mapId attempt $attemptId batch $nextBatchId.")
        }

        override def onFailure(e: Throwable): Unit = {
          pushState.exception.compareAndSet(null, new IOException("Revived PushData failed!", e))
          pushState.removeFuture(nextBatchId)
          logError(
            s"Push data to ${loc.getHost}:${loc.getPushPort} failed for map $mapId attempt $attemptId batch $nextBatchId.",
            e)
        }
      }
      val wrappedCallback: RpcResponseCallback = new RpcResponseCallback() {
        override def onSuccess(response: ByteBuffer): Unit = {
          if (response.remaining > 0) {
            val reason: Byte = response.get
            if (reason == StatusCode.SOFT_SPLIT.getValue) {
              logDebug(
                s"Push data split required for map $mapId attempt $attemptId batch $nextBatchId")
              splitPartition(shuffleId, partitionId, applicationId, loc)
              callback.onSuccess(response)
            } else {
              if (reason == StatusCode.HARD_SPLIT.getValue) {
                logDebug(s"Push data split for map $mapId attempt $attemptId batch $nextBatchId.")
                val wrappedCallbackRef = this
                pushDataRetryPool.submit(new Runnable {
                  override def run(): Unit = {
                    submitRetryPushData(
                      applicationId,
                      shuffleId,
                      mapId,
                      attemptId,
                      body,
                      nextBatchId,
                      loc,
                      wrappedCallbackRef,
                      pushState,
                      StatusCode.HARD_SPLIT)
                  }
                })
              } else {
                response.rewind
                callback.onSuccess(response)
              }
            }
          } else {
            callback.onSuccess(response)
          }
        }

        override def onFailure(e: Throwable): Unit = {
          if (pushState.exception.get != null) {
            return
          }
          logError(
            s"Push data to ${loc.getHost}:${loc.getPushPort} failed for map $mapId attempt $attemptId batch $nextBatchId.",
            e)
          // async retry push data
          if (!mapperEnded(shuffleId, mapId, attemptId)) {
            pushDataRetryPool.submit(new Runnable {
              override def run(): Unit = {
                submitRetryPushData(
                  applicationId,
                  shuffleId,
                  mapId,
                  attemptId,
                  body,
                  nextBatchId,
                  loc,
                  callback,
                  pushState,
                  getPushDataFailCause(e.getMessage))
              }
            })
          } else {
            pushState.inFlightBatches.remove(nextBatchId)
            logInfo(s"Mapper shuffleId:$shuffleId mapId:$mapId attempt:$attemptId already ended, remove batchId:$nextBatchId.")
          }
        }
      }
      // do push data
      try {
        val client: TransportClient =
          dataClientFactory.createClient(loc.getHost, loc.getPushPort, partitionId)
        val future: ChannelFuture = client.pushData(pushData, wrappedCallback)
        pushState.addFuture(nextBatchId, future)
      } catch {
        case e: Exception =>
          logError("PushData failed", e)
          wrappedCallback.onFailure(new Exception(getPushDataFailCause(e.getMessage).toString, e))
      }
    } else { // add batch data
      logDebug(s"Merge batch $nextBatchId.")
      val addressPair: String = genAddressPair(loc)
      val shoudPush: Boolean = pushState.addBatchData(addressPair, loc, nextBatchId, body)
      if (shoudPush) {
        limitMaxInFlight(mapKey, pushState, maxInFlight)
        val dataBatches: DataBatches = pushState.takeDataBatches(addressPair)
        doPushMergedData(
          addressPair.split("-")(0),
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          dataBatches.requireBatches,
          pushState,
          false)
      }
    }
    body.length
  }

  private def splitPartition(
      shuffleId: Int,
      partitionId: Int,
      applicationId: String,
      loc: PartitionLocation): Unit = {
    val splittingSet: Set[Integer] =
      splitting.computeIfAbsent(
        shuffleId,
        new Function[Integer, Set[Integer]] {
          override def apply(t: Integer): util.Set[Integer] = {
            ConcurrentHashMap.newKeySet[Integer]
          }
        })
    splittingSet.synchronized {
      if (splittingSet.contains(partitionId)) {
        logDebug(s"shuffle $shuffleId partitionId $partitionId is splitting, skip split request.")
        return
      }
      splittingSet.add(partitionId)
    }
    val currentShuffleLocs: ConcurrentHashMap[Integer, PartitionLocation] =
      reducePartitionMap.get(shuffleId)
    sendShuffleSplitAsync(
      driverRssMetaService,
      PartitionSplit(applicationId, shuffleId, partitionId, loc.getEpoch, loc),
      partitionSplitPool,
      splittingSet,
      partitionId,
      shuffleId,
      currentShuffleLocs)
  }

  private def sendShuffleSplitAsync(
      endpointRef: RpcEndpointRef,
      req: PbPartitionSplit,
      executors: ExecutorService,
      splittingSet: java.util.Set[Integer],
      partitionId: Int,
      shuffleId: Int,
      shuffleLocs: ConcurrentHashMap[Integer, PartitionLocation]): Unit = {
    endpointRef.ask[PbChangeLocationResponse](req).onComplete {
      case Success(resp) =>
        val respStatus = Utils.toStatusCode(resp.getStatus)
        if (respStatus == StatusCode.SUCCESS) {
          shuffleLocs.put(partitionId, PartitionLocation.fromPbPartitionLocation(resp.getLocation))
        } else {
          logInfo(s"split failed for $respStatus, " +
            s"shuffle file can be larger than expected, try split again");
        }
        splittingSet.remove(partitionId)
      case Failure(exception) =>
        splittingSet.remove(partitionId)
        logWarning(
          s"Shuffle file split failed for map ${shuffleId} partitionId ${partitionId}," +
            s" try again, detail : {}",
          exception);

    }(scala.concurrent.ExecutionContext.fromExecutorService(executors))
  }

  @throws[IOException]
  override def pushData(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      data: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int): Int = {
    pushOrMergeData(
      applicationId,
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      data,
      offset,
      length,
      numMappers,
      numPartitions,
      true)
  }

  @throws[IOException]
  override def prepareForMergeData(shuffleId: Int, mapId: Int, attemptId: Int): Unit = {
    val mapKey: String = Utils.makeMapKey(shuffleId, mapId, attemptId)
    val pushState: PushState = pushStates.get(mapKey)
    if (pushState != null) {
      limitMaxInFlight(mapKey, pushState, 0)
    }
  }

  @throws[IOException]
  override def mergeData(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      data: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int): Int = {
    pushOrMergeData(
      applicationId,
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      data,
      offset,
      length,
      numMappers,
      numPartitions,
      false)
  }

  @throws[IOException]
  override def pushMergedData(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int): Unit = {
    val mapKey: String = Utils.makeMapKey(shuffleId, mapId, attemptId)
    val pushState: PushState = pushStates.get(mapKey)
    if (pushState == null) {
      return
    }
    val batchesArr: ArrayList[util.Map.Entry[String, DataBatches]] =
      new ArrayList[util.Map.Entry[String, DataBatches]](pushState.batchesMap.entrySet)
    while (!batchesArr.isEmpty) {
      limitMaxInFlight(mapKey, pushState, maxInFlight)
      val entry: util.Map.Entry[String, DataBatches] = batchesArr.get(rand.nextInt(batchesArr.size))
      val batches: ArrayList[DataBatches.DataBatch] = entry.getValue.requireBatches(pushBufferSize)
      if (entry.getValue.getTotalSize == 0) {
        batchesArr.remove(entry)
      }
      val tokens: Array[String] = entry.getKey.split("-")
      doPushMergedData(
        tokens(0),
        applicationId,
        shuffleId,
        mapId,
        attemptId,
        batches,
        pushState,
        false)
    }
  }

  private def doPushMergedData(
      hostPort: String,
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      batches: ArrayList[DataBatches.DataBatch],
      pushState: PushState,
      revived: Boolean): Unit = {
    val splits: Array[String] = hostPort.split(":")
    val host: String = splits(0)
    val port: Int = splits(1).toInt
    val groupedBatchId: Int = pushState.batchId.addAndGet(1)
    pushState.inFlightBatches.put(groupedBatchId, batches.get(0).loc)
    val numBatches: Int = batches.size
    val partitionUniqueIds: Array[String] = new Array[String](numBatches)
    val offsets: Array[Int] = new Array[Int](numBatches)
    val batchIds: Array[Int] = new Array[Int](numBatches)
    var currentSize: Int = 0
    val byteBuf: CompositeByteBuf = Unpooled.compositeBuffer
    for (i <- 0 until numBatches) {
      val batch: DataBatches.DataBatch = batches.get(i)
      partitionUniqueIds(i) = batch.loc.getUniqueId
      offsets(i) = currentSize
      batchIds(i) = batch.batchId
      currentSize += batch.body.length
      byteBuf.addComponent(true, Unpooled.wrappedBuffer(batch.body))
    }
    val buffer: NettyManagedBuffer = new NettyManagedBuffer(byteBuf)
    val shuffleKey: String = Utils.makeShuffleKey(applicationId, shuffleId)
    val mergedData: PushMergedData =
      new PushMergedData(MASTER_MODE, shuffleKey, partitionUniqueIds, offsets, buffer)
    val callback: RpcResponseCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        logDebug(
          s"Push data success for map $mapId attempt $attemptId grouped batch $groupedBatchId.")
        pushState.inFlightBatches.remove(groupedBatchId)
        if (response.remaining > 0 && response.get == StatusCode.STAGE_ENDED.getValue) {
          mapperEndMap.computeIfAbsent(
            shuffleId,
            new Function[Integer, Set[String]] {
              override def apply(t: Integer): util.Set[String] = {
                ConcurrentHashMap.newKeySet[String]
              }
            }).add(Utils.makeMapKey(shuffleId, mapId, attemptId))
        }
      }

      override def onFailure(e: Throwable): Unit = {
        val errorMsg: String =
          (if (revived) {
             "Revived push"
           } else {
             "Push"
           }) + " merged data to " + host + ":" + port + " failed for map " + mapId + " attempt " + attemptId + " batches " + Arrays.toString(
            batchIds) + "."
        pushState.exception.compareAndSet(null, new IOException(errorMsg, e))
        if (log.isDebugEnabled()) {
          for (batchId <- batchIds) {
            logDebug(s"Push data failed for map $mapId attempt $attemptId batch $batchId.")
          }
        }
      }
    }
    val wrappedCallback: RpcResponseCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        callback.onSuccess(response)
      }

      override def onFailure(e: Throwable): Unit = {
        if (pushState.exception.get != null) {
          return
        }
        if (revived) {
          callback.onFailure(e)
          return
        }
        logError(
          (if (revived) {
             "Revived push"
           } else {
             "Push"
           }) + " merged data to " + host + ":" + port + " failed for map " + mapId + " attempt " + attemptId + " batches " + Arrays.toString(
            batchIds) + ".",
          e)
        if (!mapperEnded(shuffleId, mapId, attemptId)) {
          pushDataRetryPool.submit(new Runnable {
            override def run(): Unit = {
              submitRetryPushMergedData(
                pushState,
                applicationId,
                shuffleId,
                mapId,
                attemptId,
                batches,
                getPushDataFailCause(e.getMessage),
                groupedBatchId)
            }
          })
        }
      }
    }
    // do push merged data
    try {
      val client: TransportClient = dataClientFactory.createClient(host, port)
      client.pushMergedData(mergedData, wrappedCallback)
    } catch {
      case e: Exception =>
        logError("PushMergedData failed", e)
        wrappedCallback.onFailure(new Exception(getPushDataFailCause(e.getMessage).toString, e))
    }
  }

  @throws[IOException]
  override def mapperEnd(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int): Unit = {
    val mapKey: String = Utils.makeMapKey(shuffleId, mapId, attemptId)
    val pushState: PushState =
      pushStates.computeIfAbsent(mapKey, new Function[String, PushState] {
        override def apply(t: String): PushState = {
          new PushState(conf)
        }
      })
    try {
      limitMaxInFlight(mapKey, pushState, 0)
      val response: MapperEndResponse = driverRssMetaService.askSync[MapperEndResponse](MapperEnd(
        applicationId,
        shuffleId,
        mapId,
        attemptId,
        numMappers))
      if (response.status ne StatusCode.SUCCESS) {
        throw new IOException("MapperEnd failed! StatusCode: " + response.status)
      }
    } finally {
      pushStates.remove(mapKey)
    }
  }

  override def cleanup(applicationId: String, shuffleId: Int, mapId: Int, attemptId: Int): Unit = {
    val mapKey: String = Utils.makeMapKey(shuffleId, mapId, attemptId)
    val pushState: PushState = pushStates.remove(mapKey)
    if (pushState != null) {
      pushState.exception.compareAndSet(null, new IOException("Cleaned Up"))
      pushState.cancelFutures()
    }
  }

  override def unregisterShuffle(
      applicationId: String,
      shuffleId: Int,
      isDriver: Boolean): Boolean = {
    if (isDriver) {
      try driverRssMetaService.send(UnregisterShuffle(
        applicationId,
        shuffleId,
        RssHARetryClient.genRequestId))
      catch {
        case e: Exception =>
          // If some exceptions need to be ignored, they shouldn't be logged as error-level,
          // otherwise it will mislead users.
          logError("Send UnregisterShuffle failed, ignore.", e)
      }
    }
    // clear status
    reducePartitionMap.remove(shuffleId)
    reduceFileGroupsMap.remove(shuffleId)
    mapperEndMap.remove(shuffleId)
    splitting.remove(shuffleId)
    logInfo(s"Unregistered shuffle $shuffleId.")
    true
  }

  @throws[IOException]
  override def readPartition(
      applicationId: String,
      shuffleId: Int,
      partitionId: Int,
      attemptNumber: Int): RssInputStream = {
    readPartition(applicationId, shuffleId, partitionId, attemptNumber, 0, Integer.MAX_VALUE)
  }

  @throws[IOException]
  override def readPartition(
      applicationId: String,
      shuffleId: Int,
      partitionId: Int,
      attemptNumber: Int,
      startMapIndex: Int,
      endMapIndex: Int): RssInputStream = {
    val shuffleKey: String = Utils.makeShuffleKey(applicationId, shuffleId)
    val fileGroups: ReduceFileGroups = reduceFileGroupsMap.computeIfAbsent(
      shuffleId,
      new Function[Integer, ReduceFileGroups] {
        override def apply(id: Integer): ReduceFileGroups = {
          def foo(id: Integer): ReduceFileGroups = {
            val getReducerFileGroupStartTime: Long = System.nanoTime
            try {
              if (driverRssMetaService == null) {
                logWarning("Driver endpoint is null!")
                return null
              }
              val getReducerFileGroup: GetReducerFileGroup =
                GetReducerFileGroup(applicationId, shuffleId)
              val response =
                driverRssMetaService.askSync[GetReducerFileGroupResponse](getReducerFileGroup)
              if (response.status eq StatusCode.SUCCESS) {
                logInfo(s"Shuffle $shuffleId request reducer file group success using time:${(System.nanoTime - getReducerFileGroupStartTime) / 1000000} ms")
                return new ReduceFileGroups(response.fileGroup, response.attempts)
              } else {
                if (response.status eq StatusCode.STAGE_END_TIME_OUT) {
                  logWarning(
                    s"Request $getReducerFileGroup return ${StatusCode.STAGE_END_TIME_OUT.toString} for $shuffleKey")
                } else {
                  if (response.status eq StatusCode.SHUFFLE_DATA_LOST) {
                    logWarning(
                      s"Request $getReducerFileGroup return ${StatusCode.SHUFFLE_DATA_LOST.toString} for $shuffleKey")
                  }
                }
              }
            } catch {
              case e: Exception =>
                logError(s"Exception raised while call GetReducerFileGroup for $shuffleKey.", e)
            }
            null
          }

          foo(id)
        }
      })
    if (fileGroups == null) {
      val msg: String = s"Shuffle data lost for shuffle $shuffleId reduce $partitionId!"
      logError(msg)
      throw new IOException(msg)
    } else {
      if (fileGroups.partitionGroups.length == 0) {
        logWarning(s"Shuffle data is empty for shuffle $shuffleId reduce $partitionId.")
        RssInputStream.empty
      } else {
        RssInputStream.create(
          conf,
          dataClientFactory,
          shuffleKey,
          fileGroups.partitionGroups(partitionId),
          fileGroups.mapAttempts,
          attemptNumber,
          startMapIndex,
          endMapIndex)
      }
    }
  }

  override def shutDown(): Unit = {
    if (null != rpcEnv) {
      rpcEnv.shutdown()
    }
    if (null != dataClientFactory) {
      dataClientFactory.close()
    }
    if (null != pushDataRetryPool) {
      pushDataRetryPool.shutdown()
    }
    if (null != partitionSplitPool) {
      partitionSplitPool.shutdown()
    }
    if (null != driverRssMetaService) {
      driverRssMetaService = null
    }
    logWarning("Shuffle client has been shutdown!")
  }

  override def setupMetaServiceRef(host: String, port: Int): Unit = {
    driverRssMetaService =
      rpcEnv.setupEndpointRef(new RpcAddress(host, port), RpcNameConstants.RSS_METASERVICE_EP)
  }

  override def setupMetaServiceRef(endpointRef: RpcEndpointRef): Unit = {
    driverRssMetaService = endpointRef
  }

  private def getLocalHost: String = {
    if (ia == null) {
      try ia = InetAddress.getLocalHost
      catch {
        case e: UnknownHostException =>
          logError("Unknown host", e)
          return null
      }
    }
    ia.getHostName
  }

  private def mapperEnded(shuffleId: Int, mapId: Int, attemptId: Int): Boolean = {
    mapperEndMap.containsKey(shuffleId) && mapperEndMap.get(shuffleId).contains(Utils.makeMapKey(
      shuffleId,
      mapId,
      attemptId))
  }

  private def getPushDataFailCause(message: String): StatusCode = {
    logInfo("[getPushDataFailCause] message: " + message)
    var cause: StatusCode = null
    if (StatusCode.PUSH_DATA_FAIL_SLAVE.getMessage == message) {
      cause = StatusCode.PUSH_DATA_FAIL_SLAVE
    } else {
      if (StatusCode.PUSH_DATA_FAIL_MAIN.getMessage == message || connectFail(message)) {
        cause = StatusCode.PUSH_DATA_FAIL_MAIN
      } else {
        cause = StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE
      }
    }
    cause
  }

  private def connectFail(message: String): Boolean = {
    (message.startsWith("Connection from ") && message.endsWith(
      " closed")) || (message == "Connection reset by peer") || (message.startsWith(
      "Failed to send RPC "))
  }
}

object ShuffleClientImpl {
  private val MASTER_MODE = PartitionLocation.Mode.MASTER.mode

  private val rand = new Random

  private class ReduceFileGroups private[client] (
      val partitionGroups: Array[Array[PartitionLocation]],
      val mapAttempts: Array[Int])
}
