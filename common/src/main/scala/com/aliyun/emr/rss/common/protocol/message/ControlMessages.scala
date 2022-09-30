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

package com.aliyun.emr.rss.common.protocol.message

import java.util
import java.util.UUID

import scala.collection.JavaConverters._

import org.roaringbitmap.RoaringBitmap

import com.aliyun.emr.rss.common.exception.RssException
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{DiskInfo, WorkerInfo}
import com.aliyun.emr.rss.common.network.protocol.TransportMessage
import com.aliyun.emr.rss.common.protocol._
import com.aliyun.emr.rss.common.protocol.MessageType._
import com.aliyun.emr.rss.common.util.{PbSerDeUtils, Utils}

sealed trait Message extends Serializable

sealed trait MasterMessage extends Message

sealed abstract class MasterRequestMessage extends MasterMessage {
  var requestId: String

  def requestId_(id: String): Unit = {
    this.requestId = id
  }
}

sealed trait WorkerMessage extends Message

sealed trait ClientMessage extends Message

object ControlMessages extends Logging {
  val ZERO_UUID = new UUID(0L, 0L).toString

  type WorkerResource = java.util.HashMap[
    WorkerInfo,
    (java.util.List[PartitionLocation], java.util.List[PartitionLocation])]

  /**
   * ==========================================
   *         handled by master
   *  ==========================================
   */
  val pbCheckForWorkerTimeout: PbCheckForWorkerTimeout =
    PbCheckForWorkerTimeout.newBuilder().build()

  case object CheckForApplicationTimeOut extends Message

  case object RemoveExpiredShuffle extends Message

  /**
   * The response message for one-way message. Due to the Master HA, we must know whether a Master
   * is the leader, and the one-way message does not care about the response, so there is no
   * corresponding response message. So add a response message to get the response.
   */
  case object OneWayMessageResponse extends Message

  def pbRegisterWorker(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      disks: Map[String, DiskInfo],
      userResourceUsage: Map[UserIdentifier, ResourceConsumption],
      requestId: String): PbRegisterWorker = {
    val pbDisks = disks.mapValues(PbSerDeUtils.toPbDiskInfo).asJava
    val pbUserResourceUsage = userResourceUsage
      .map { case (userIdentifier, resourceConsumption) => (userIdentifier.toString, resourceConsumption) }
      .mapValues(PbSerDeUtils.toPbResourceConsumption)
      .asJava
    PbRegisterWorker.newBuilder()
      .setHost(host)
      .setRpcPort(rpcPort)
      .setPushPort(pushPort)
      .setFetchPort(fetchPort)
      .setReplicatePort(replicatePort)
      .putAllDisks(pbDisks)
      .putAllUserResourceUsage(pbUserResourceUsage)
      .setRequestId(requestId)
      .build()
  }

  case class HeartbeatFromWorker(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      disks: util.Map[String, DiskInfo],
      userResourceUsage: util.Map[UserIdentifier, ResourceConsumption],
      shuffleKeys: util.HashSet[String],
      override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  case class HeartbeatResponse(
      expiredShuffleKeys: util.HashSet[String],
      registered: Boolean) extends MasterMessage

  case class RegisterShuffle(
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int)
    extends MasterMessage

  case class RegisterShuffleResponse(
      status: StatusCode,
      partitionLocations: util.List[PartitionLocation])
    extends MasterMessage

  case class RequestSlots(
      applicationId: String,
      shuffleId: Int,
      partitionIdList: util.ArrayList[Integer],
      hostname: String,
      shouldReplicate: Boolean,
      userIdentifier: UserIdentifier,
      override var requestId: String = ZERO_UUID)
    extends MasterRequestMessage

  case class ReleaseSlots(
      applicationId: String,
      shuffleId: Int,
      workerIds: util.List[String],
      slots: util.List[util.Map[String, Integer]],
      override var requestId: String = ZERO_UUID)
    extends MasterRequestMessage

  case class ReleaseSlotsResponse(status: StatusCode)
    extends MasterMessage

  case class RequestSlotsResponse(
      status: StatusCode,
      workerResource: WorkerResource)
    extends MasterMessage

  trait ChangeLocationRequest

  case class Revive(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      epoch: Int,
      oldPartition: PartitionLocation,
      cause: StatusCode)
    extends MasterMessage with ChangeLocationRequest

  case class PartitionSplit(
      applicationId: String,
      shuffleId: Int,
      partitionId: Int,
      epoch: Int,
      oldPartition: PartitionLocation)
    extends MasterMessage with ChangeLocationRequest

  case class ChangeLocationResponse(
      status: StatusCode,
      partition: PartitionLocation)
    extends MasterMessage

  case class MapperEnd(
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int)
    extends MasterMessage

  case class MapperEndResponse(status: StatusCode) extends MasterMessage

  case class GetReducerFileGroup(applicationId: String, shuffleId: Int) extends MasterMessage

  // util.Set[String] -> util.Set[Path.toString]
  // Path can't be serialized
  case class GetReducerFileGroupResponse(
      status: StatusCode,
      fileGroup: Array[Array[PartitionLocation]],
      attempts: Array[Int])
    extends MasterMessage

  case class WorkerLost(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  case class WorkerLostResponse(success: Boolean) extends MasterMessage

  case class StageEnd(applicationId: String, shuffleId: Int) extends MasterMessage

  case class StageEndResponse(status: StatusCode)
    extends MasterMessage

  case class UnregisterShuffle(
      appId: String,
      shuffleId: Int,
      override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  case class UnregisterShuffleResponse(status: StatusCode) extends MasterMessage

  case class ApplicationLost(
      appId: String,
      override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  case class ApplicationLostResponse(status: StatusCode) extends MasterMessage

  case class HeartbeatFromApplication(
      appId: String,
      totalWritten: Long,
      fileCount: Long,
      override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  case class GetBlacklist(localBlacklist: util.List[WorkerInfo]) extends MasterMessage

  case class GetBlacklistResponse(
      statusCode: StatusCode,
      blacklist: util.List[WorkerInfo],
      unknownWorkers: util.List[WorkerInfo]) extends Message

  case class CheckQuota(userIdentifier: UserIdentifier) extends Message

  case class CheckQuotaResponse(isAvailable: Boolean) extends Message

  case class ReportWorkerFailure(
      failed: util.List[WorkerInfo],
      override var requestId: String = ZERO_UUID) extends MasterRequestMessage

  /**
   * ==========================================
   *         handled by worker
   *  ==========================================
   */
  def pbRegisterWorkerResponse(success: Boolean, message: String): PbRegisterWorkerResponse =
    PbRegisterWorkerResponse.newBuilder()
      .setSuccess(success)
      .setMessage(message)
      .build()

  case class ReregisterWorkerResponse(success: Boolean) extends WorkerMessage

  case class ReserveSlots(
      applicationId: String,
      shuffleId: Int,
      masterLocations: util.List[PartitionLocation],
      slaveLocations: util.List[PartitionLocation],
      splitThreshold: Long,
      splitMode: PartitionSplitMode,
      partitionType: PartitionType,
      rangeReadFilter: Boolean,
      userIdentifier: UserIdentifier)
    extends WorkerMessage

  case class ReserveSlotsResponse(
      status: StatusCode,
      reason: String = "") extends WorkerMessage

  case class CommitFiles(
      applicationId: String,
      shuffleId: Int,
      masterIds: util.List[String],
      slaveIds: util.List[String],
      mapAttempts: Array[Int])
    extends WorkerMessage

  case class CommitFilesResponse(
      status: StatusCode,
      committedMasterIds: util.List[String],
      committedSlaveIds: util.List[String],
      failedMasterIds: util.List[String],
      failedSlaveIds: util.List[String],
      committedMasterStorageInfos: util.Map[String, StorageInfo] =
        Map.empty[String, StorageInfo].asJava,
      committedSlaveStorageInfos: util.Map[String, StorageInfo] =
        Map.empty[String, StorageInfo].asJava,
      committedMapIdBitMap: util.Map[String, RoaringBitmap] =
        Map.empty[String, RoaringBitmap].asJava,
      totalWritten: Long = 0,
      fileCount: Int = 0) extends WorkerMessage

  case class Destroy(
      shuffleKey: String,
      masterLocations: util.List[String],
      slaveLocations: util.List[String])
    extends WorkerMessage

  case class DestroyResponse(
      status: StatusCode,
      failedMasters: util.List[String],
      failedSlaves: util.List[String])
    extends WorkerMessage

  /**
   * ==========================================
   *              common
   *  ==========================================
   */
  case class SlaveLostResponse(status: StatusCode, slaveLocation: PartitionLocation) extends Message

  case object GetWorkerInfos extends Message

  case class GetWorkerInfosResponse(status: StatusCode, workerInfos: WorkerInfo*) extends Message

  case object ThreadDump extends Message

  case class ThreadDumpResponse(threadDump: String) extends Message

  case class UserIdentifier(tenantId: String, name: String) extends Message {
    assert(
      tenantId != null && tenantId.nonEmpty,
      "UserIdentifier's tenantId should not be null or empty.")
    assert(name != null && name.nonEmpty, "UserIdentifier's name should not be null or empty.")

    override def toString: String = {
      s"`$tenantId`.`$name`"
    }
  }

  object UserIdentifier {
    val USER_IDENTIFIER = "^\\`(.+)\\`\\.\\`(.+)\\`$".r

    def apply(userIdentifier: String): UserIdentifier = {
      if (USER_IDENTIFIER.findPrefixOf(userIdentifier).isDefined) {
        val USER_IDENTIFIER(tenantId, name) = userIdentifier
        UserIdentifier(tenantId, name)
      } else {
        logError(s"Failed to parse user identifier: $userIdentifier")
        throw new RssException(s"Failed to parse user identifier: ${userIdentifier}")
      }
    }
  }

  case class ResourceConsumption(
      diskBytesWritten: Long,
      diskFileCount: Long,
      hdfsBytesWritten: Long,
      hdfsFileCount: Long)

  // TODO change message type to GeneratedMessageV3
  def toTransportMessage(message: Any): TransportMessage = message match {
    case _: PbCheckForWorkerTimeoutOrBuilder =>
      new TransportMessage(MessageType.CHECK_FOR_WORKER_TIMEOUT, null)

    case CheckForApplicationTimeOut =>
      new TransportMessage(MessageType.CHECK_FOR_APPLICATION_TIMEOUT, null)

    case RemoveExpiredShuffle =>
      new TransportMessage(MessageType.REMOVE_EXPIRED_SHUFFLE, null)

    case pb: PbRegisterWorker =>
      new TransportMessage(MessageType.REGISTER_WORKER, pb.toByteArray)

    case HeartbeatFromWorker(
          host,
          rpcPort,
          pushPort,
          fetchPort,
          replicatePort,
          disks,
          userResourceUsage,
          shuffleKeys,
          requestId) =>
      val pbDisks = disks.asScala.mapValues(PbSerDeUtils.toPbDiskInfo).asJava
      val pbUserResourceUsage = userResourceUsage
        .asScala
        .map { case (userIdentifier, resourceConsumption) => (userIdentifier.toString, resourceConsumption) }
        .mapValues(PbSerDeUtils.toPbResourceConsumption)
        .asJava
      val payload = PbHeartbeatFromWorker.newBuilder()
        .setHost(host)
        .setRpcPort(rpcPort)
        .setPushPort(pushPort)
        .setFetchPort(fetchPort)
        .putAllDisks(pbDisks)
        .putAllUserResourceUsage(pbUserResourceUsage)
        .setReplicatePort(replicatePort)
        .addAllShuffleKeys(shuffleKeys)
        .setRequestId(requestId)
        .build().toByteArray
      new TransportMessage(MessageType.HEARTBEAT_FROM_WORKER, payload)

    case HeartbeatResponse(expiredShuffleKeys, registered) =>
      val payload = PbHeartbeatResponse.newBuilder()
        .addAllExpiredShuffleKeys(expiredShuffleKeys)
        .setRegistered(registered)
        .build().toByteArray
      new TransportMessage(MessageType.HEARTBEAT_RESPONSE, payload)

    case RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions) =>
      val payload = PbRegisterShuffle.newBuilder()
        .setApplicationId(applicationId)
        .setShuffleId(shuffleId)
        .setNumMapppers(numMappers)
        .setNumPartitions(numPartitions)
        .build().toByteArray
      new TransportMessage(MessageType.REGISTER_SHUFFLE, payload)

    case RegisterShuffleResponse(status, partitionLocations) =>
      val builder = PbRegisterShuffleResponse.newBuilder()
        .setStatus(status.getValue)
      if (!partitionLocations.isEmpty) {
        builder.addAllPartitionLocations(partitionLocations.iterator().asScala
          .map(PartitionLocation.toPbPartitionLocation).toList.asJava)
      }
      val payload = builder.build().toByteArray
      new TransportMessage(MessageType.REGISTER_SHUFFLE_RESPONSE, payload)

    case RequestSlots(
          applicationId,
          shuffleId,
          partitionIdList,
          hostname,
          shouldReplicate,
          userIdentifier,
          requestId) =>
      val payload = PbRequestSlots.newBuilder()
        .setApplicationId(applicationId)
        .setShuffleId(shuffleId)
        .addAllPartitionIdList(partitionIdList)
        .setHostname(hostname)
        .setShouldReplicate(shouldReplicate)
        .setRequestId(requestId)
        .setUserIdentifier(
          PbUserIdentifier
            .newBuilder()
            .setTenantId(userIdentifier.tenantId)
            .setName(userIdentifier.name))
        .build().toByteArray
      new TransportMessage(MessageType.REQUEST_SLOTS, payload)

    case ReleaseSlots(applicationId, shuffleId, workerIds, slots, requestId) =>
      val pbSlots = slots.asScala.map(slot =>
        PbSlotInfo.newBuilder().putAllSlot(slot).build()).toList
      val payload = PbReleaseSlots.newBuilder()
        .setApplicationId(applicationId)
        .setShuffleId(shuffleId)
        .setRequestId(requestId)
        .addAllWorkerIds(workerIds)
        .addAllSlots(pbSlots.asJava)
        .build().toByteArray
      new TransportMessage(MessageType.RELEASE_SLOTS, payload)

    case ReleaseSlotsResponse(status) =>
      val payload = PbReleaseSlotsResponse.newBuilder()
        .setStatus(status.getValue).build().toByteArray
      new TransportMessage(MessageType.RELEASE_SLOTS_RESPONSE, payload)

    case RequestSlotsResponse(status, workerResource) =>
      val builder = PbRequestSlotsResponse.newBuilder()
        .setStatus(status.getValue)
      if (!workerResource.isEmpty) {
        builder.putAllWorkerResource(
          Utils.convertWorkerResourceToPbWorkerResource(workerResource))
      }
      val payload = builder.build().toByteArray
      new TransportMessage(MessageType.REQUEST_SLOTS_RESPONSE, payload)

    case Revive(
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          epoch,
          oldPartition,
          cause) =>
      val builder = PbRevive.newBuilder()
      builder.setApplicationId(applicationId)
        .setShuffleId(shuffleId)
        .setMapId(mapId)
        .setAttemptId(attemptId)
        .setPartitionId(partitionId)
        .setEpoch(epoch)
        .setStatus(cause.getValue)
      if (oldPartition != null) {
        builder.setOldPartition(PartitionLocation.toPbPartitionLocation(oldPartition))
      }
      val payload = builder.build().toByteArray
      new TransportMessage(MessageType.REVIVE, payload)

    case ChangeLocationResponse(status, location) =>
      val builder = PbChangeLocationResponse.newBuilder()
        .setStatus(status.getValue)
      if (location != null) {
        builder.setLocation(PartitionLocation.toPbPartitionLocation(location))
      }
      val payload = builder.build().toByteArray
      new TransportMessage(MessageType.CHANGE_LOCATION_RESPONSE, payload)

    case MapperEnd(applicationId, shuffleId, mapId, attemptId, numMappers) =>
      val payload = PbMapperEnd.newBuilder()
        .setApplicationId(applicationId)
        .setShuffleId(shuffleId)
        .setMapId(mapId)
        .setAttemptId(attemptId)
        .setNumMappers(numMappers)
        .build().toByteArray
      new TransportMessage(MessageType.MAPPER_END, payload)

    case MapperEndResponse(status) =>
      val payload = PbMapperEndResponse.newBuilder()
        .setStatus(status.getValue)
        .build().toByteArray
      new TransportMessage(MessageType.MAPPER_END_RESPONSE, payload)

    case GetReducerFileGroup(applicationId, shuffleId) =>
      val payload = PbGetReducerFileGroup.newBuilder()
        .setApplicationId(applicationId).setShuffleId(shuffleId)
        .build().toByteArray
      new TransportMessage(MessageType.GET_REDUCER_FILE_GROUP, payload)

    case GetReducerFileGroupResponse(status, fileGroup, attempts) =>
      val builder = PbGetReducerFileGroupResponse
        .newBuilder()
        .setStatus(status.getValue)
      builder.addAllFileGroup(
        fileGroup.map { arr =>
          PbFileGroup.newBuilder().addAllLocations(arr
            .map(PartitionLocation.toPbPartitionLocation).toIterable.asJava).build()
        }
          .toIterable
          .asJava)
      builder.addAllAttempts(attempts.map(new Integer(_)).toIterable.asJava)
      val payload = builder.build().toByteArray
      new TransportMessage(MessageType.GET_REDUCER_FILE_GROUP_RESPONSE, payload)

    case WorkerLost(host, rpcPort, pushPort, fetchPort, replicatePort, requestId) =>
      val payload = PbWorkerLost.newBuilder()
        .setHost(host)
        .setRpcPort(rpcPort)
        .setPushPort(pushPort)
        .setFetchPort(fetchPort)
        .setReplicatePort(replicatePort)
        .setRequestId(requestId)
        .build().toByteArray
      new TransportMessage(MessageType.WORKER_LOST, payload)

    case WorkerLostResponse(success) =>
      val payload = PbWorkerLostResponse.newBuilder()
        .setSuccess(success)
        .build().toByteArray
      new TransportMessage(MessageType.WORKER_LOST_RESPONSE, payload)

    case StageEnd(applicationId, shuffleId) =>
      val payload = PbStageEnd.newBuilder()
        .setApplicationId(applicationId)
        .setShuffleId(shuffleId)
        .build().toByteArray
      new TransportMessage(MessageType.STAGE_END, payload)

    case StageEndResponse(status) =>
      val payload = PbStageEndResponse.newBuilder()
        .setStatus(status.getValue)
        .build().toByteArray
      new TransportMessage(MessageType.STAGE_END_RESPONSE, payload)

    case UnregisterShuffle(appId, shuffleId, requestId) =>
      val payload = PbUnregisterShuffle.newBuilder()
        .setAppId(appId).setShuffleId(shuffleId).setRequestId(requestId)
        .build().toByteArray
      new TransportMessage(MessageType.UNREGISTER_SHUFFLE, payload)

    case UnregisterShuffleResponse(status) =>
      val payload = PbUnregisterShuffleResponse.newBuilder()
        .setStatus(status.getValue)
        .build().toByteArray
      new TransportMessage(MessageType.UNREGISTER_SHUFFLE_RESPONSE, payload)

    case ApplicationLost(appId, requestId) =>
      val payload = PbApplicationLost.newBuilder()
        .setAppId(appId).setRequestId(requestId)
        .build().toByteArray
      new TransportMessage(MessageType.APPLICATION_LOST, payload)

    case ApplicationLostResponse(status) =>
      val payload = PbApplicationLostResponse.newBuilder()
        .setStatus(status.getValue).build().toByteArray
      new TransportMessage(MessageType.APPLICATION_LOST_RESPONSE, payload)

    case HeartbeatFromApplication(appId, totalWritten, fileCount, requestId) =>
      val payload = PbHeartbeatFromApplication.newBuilder()
        .setAppId(appId)
        .setRequestId(requestId)
        .setTotalWritten(totalWritten)
        .setFileCount(fileCount)
        .build().toByteArray
      new TransportMessage(MessageType.HEARTBEAT_FROM_APPLICATION, payload)

    case GetBlacklist(localBlacklist) =>
      val payload = PbGetBlacklist.newBuilder()
        .addAllLocalBlackList(localBlacklist.asScala.map(WorkerInfo.toPbWorkerInfo)
          .toList.asJava)
        .build().toByteArray
      new TransportMessage(MessageType.GET_BLACKLIST, payload)

    case GetBlacklistResponse(statusCode, blacklist, unknownWorkers) =>
      val builder = PbGetBlacklistResponse.newBuilder()
        .setStatus(statusCode.getValue)
      builder.addAllBlacklist(blacklist.asScala.map(WorkerInfo.toPbWorkerInfo).toList.asJava)
      builder.addAllUnknownWorkers(
        unknownWorkers.asScala.map(WorkerInfo.toPbWorkerInfo).toList.asJava)
      val payload = builder.build().toByteArray
      new TransportMessage(MessageType.GET_BLACKLIST_RESPONSE, payload)

    case CheckQuota(userIdentifier) =>
      val builder = PbCheckQuota.newBuilder()
        .setUserIdentifier(
          PbUserIdentifier
            .newBuilder()
            .setTenantId(userIdentifier.tenantId)
            .setName(userIdentifier.name))
      new TransportMessage(
        MessageType.CHECK_QUOTA,
        builder.build().toByteArray)

    case CheckQuotaResponse(available) =>
      val payload = PbCheckQuotaResponse.newBuilder()
        .setAvailable(available)
        .build().toByteArray
      new TransportMessage(MessageType.CHECK_QUOTA_RESPONSE, payload)

    case ReportWorkerFailure(failed, requestId) =>
      val payload = PbReportWorkerFailure.newBuilder()
        .addAllFailed(failed.asScala.map(WorkerInfo.toPbWorkerInfo(_)).toList.asJava)
        .setRequestId(requestId).build().toByteArray
      new TransportMessage(MessageType.REPORT_WORKER_FAILURE, payload)

    case pb: PbRegisterWorkerResponse =>
      new TransportMessage(MessageType.REGISTER_WORKER_RESPONSE, pb.toByteArray)

    case ReregisterWorkerResponse(success) =>
      val payload = PbReregisterWorkerResponse.newBuilder()
        .setSuccess(success)
        .build().toByteArray
      new TransportMessage(MessageType.REREGISTER_WORKER_RESPONSE, payload)

    case ReserveSlots(
          applicationId,
          shuffleId,
          masterLocations,
          slaveLocations,
          splitThreshold,
          splitMode,
          partType,
          rangeReadFilter,
          userIdentifier) =>
      val payload = PbReserveSlots.newBuilder()
        .setApplicationId(applicationId)
        .setShuffleId(shuffleId)
        .addAllMasterLocations(masterLocations.asScala
          .map(PartitionLocation.toPbPartitionLocation(_)).toList.asJava)
        .addAllSlaveLocations(slaveLocations.asScala
          .map(PartitionLocation.toPbPartitionLocation(_)).toList.asJava)
        .setSplitThreshold(splitThreshold)
        .setSplitMode(splitMode.getValue)
        .setPartitionType(partType.getValue)
        .setRangeReadFilter(rangeReadFilter)
        .setUserIdentifier(
          PbUserIdentifier
            .newBuilder()
            .setTenantId(userIdentifier.tenantId)
            .setName(userIdentifier.name))
        .build().toByteArray
      new TransportMessage(MessageType.RESERVE_SLOTS, payload)

    case ReserveSlotsResponse(status, reason) =>
      val payload = PbReserveSlotsResponse.newBuilder()
        .setStatus(status.getValue).setReason(reason)
        .build().toByteArray
      new TransportMessage(MessageType.RESERVE_SLOTS_RESPONSE, payload)

    case CommitFiles(applicationId, shuffleId, masterIds, slaveIds, mapAttempts) =>
      val payload = PbCommitFiles.newBuilder()
        .setApplicationId(applicationId)
        .setShuffleId(shuffleId)
        .addAllMasterIds(masterIds)
        .addAllSlaveIds(slaveIds)
        .addAllMapAttempts(mapAttempts.map(new Integer(_)).toIterable.asJava)
        .build().toByteArray
      new TransportMessage(MessageType.COMMIT_FILES, payload)

    case CommitFilesResponse(
          status,
          committedMasterIds,
          committedSlaveIds,
          failedMasterIds,
          failedSlaveIds,
          committedMasterStorageInfos,
          committedSlaveStorageInfos,
          committedMapIdBitMap,
          totalWritten,
          fileCount) =>
      val builder = PbCommitFilesResponse.newBuilder()
        .setStatus(status.getValue)
      builder.addAllCommittedMasterIds(committedMasterIds)
      builder.addAllCommittedSlaveIds(committedSlaveIds)
      builder.addAllFailedMasterIds(failedMasterIds)
      builder.addAllFailedSlaveIds(failedSlaveIds)
      committedMasterStorageInfos.asScala.foreach(entry =>
        builder.putCommittedMasterStorageInfos(entry._1, StorageInfo.toPb(entry._2)))
      committedSlaveStorageInfos.asScala.foreach(entry =>
        builder.putCommittedSlaveStorageInfos(entry._1, StorageInfo.toPb(entry._2)))
      committedMapIdBitMap.asScala.foreach(entry => {
        builder.putMapIdBitmap(entry._1, Utils.roaringBitmapToByteString(entry._2))
      })
      builder.setTotalWritten(totalWritten)
      builder.setFileCount(fileCount)
      val payload = builder.build().toByteArray
      new TransportMessage(MessageType.COMMIT_FILES_RESPONSE, payload)

    case Destroy(shuffleKey, masterLocations, slaveLocations) =>
      val payload = PbDestroy.newBuilder()
        .setShuffleKey(shuffleKey)
        .addAllMasterLocations(masterLocations)
        .addAllSlaveLocation(slaveLocations)
        .build().toByteArray
      new TransportMessage(MessageType.DESTROY, payload)

    case DestroyResponse(status, failedMasters, failedSlaves) =>
      val builder = PbDestroyResponse.newBuilder()
        .setStatus(status.getValue)
      builder.addAllFailedMasters(failedMasters)
      builder.addAllFailedSlaves(failedSlaves)
      val payload = builder.build().toByteArray
      new TransportMessage(MessageType.DESTROY_RESPONSE, payload)

    case SlaveLostResponse(status, slaveLocation) =>
      val payload = PbSlaveLostResponse.newBuilder()
        .setStatus(status.getValue)
        .setSlaveLocation(PartitionLocation.toPbPartitionLocation(slaveLocation))
        .build().toByteArray
      new TransportMessage(MessageType.SLAVE_LOST_RESPONSE, payload)

    case GetWorkerInfos =>
      new TransportMessage(MessageType.GET_WORKER_INFO, null)

    case GetWorkerInfosResponse(status, workerInfos @ _*) =>
      val payload = PbGetWorkerInfosResponse.newBuilder()
        .setStatus(status.getValue)
        .addAllWorkerInfos(workerInfos.map(WorkerInfo.toPbWorkerInfo(_)).toList.asJava)
        .build().toByteArray
      new TransportMessage(MessageType.GET_WORKER_INFO_RESPONSE, payload)

    case ThreadDump =>
      new TransportMessage(MessageType.THREAD_DUMP, null)

    case ThreadDumpResponse(threadDump) =>
      val payload = PbThreadDumpResponse.newBuilder()
        .setThreadDump(threadDump).build().toByteArray
      new TransportMessage(MessageType.THREAD_DUMP_RESPONSE, payload)

    case PartitionSplit(applicationId, shuffleId, partitionId, epoch, oldPartition) =>
      val payload = PbPartitionSplit.newBuilder()
        .setApplicationId(applicationId).setShuffleId(shuffleId).setPartitionId(partitionId)
        .setEpoch(epoch).setOldPartition(PartitionLocation.toPbPartitionLocation(oldPartition))
        .build().toByteArray
      new TransportMessage(MessageType.PARTITION_SPLIT, payload)

    case OneWayMessageResponse =>
      new TransportMessage(MessageType.ONE_WAY_MESSAGE_RESPONSE, null)
  }

  // TODO change return type to GeneratedMessageV3
  def fromTransportMessage(message: TransportMessage): Any = {
    message.getType match {
      case UNKNOWN_MESSAGE | UNRECOGNIZED =>
        val msg = s"received unknown message $message"
        logError(msg)
        throw new UnsupportedOperationException(msg)

      case REGISTER_WORKER =>
        PbRegisterWorker.parseFrom(message.getPayload)

      case HEARTBEAT_FROM_WORKER =>
        val pbHeartbeatFromWorker = PbHeartbeatFromWorker.parseFrom(message.getPayload)
        val shuffleKeys = new util.HashSet[String]()
        val disks =
          pbHeartbeatFromWorker.getDisksMap.asScala.mapValues(PbSerDeUtils.fromPbDiskInfo).asJava
        val userResourceUsage = pbHeartbeatFromWorker
          .getUserResourceUsageMap
          .asScala
          .map(x => (UserIdentifier(x._1), x._2))
          .mapValues(PbSerDeUtils.fromPbResourceConsumption)
          .toMap
          .asJava
        if (pbHeartbeatFromWorker.getShuffleKeysCount > 0) {
          shuffleKeys.addAll(pbHeartbeatFromWorker.getShuffleKeysList)
        }
        HeartbeatFromWorker(
          pbHeartbeatFromWorker.getHost,
          pbHeartbeatFromWorker.getRpcPort,
          pbHeartbeatFromWorker.getPushPort,
          pbHeartbeatFromWorker.getFetchPort,
          pbHeartbeatFromWorker.getReplicatePort,
          disks,
          userResourceUsage,
          shuffleKeys,
          pbHeartbeatFromWorker.getRequestId)

      case HEARTBEAT_RESPONSE =>
        val pbHeartbeatResponse = PbHeartbeatResponse.parseFrom(message.getPayload)
        val expiredShuffleKeys = new util.HashSet[String]()
        if (pbHeartbeatResponse.getExpiredShuffleKeysCount > 0) {
          expiredShuffleKeys.addAll(pbHeartbeatResponse.getExpiredShuffleKeysList)
        }
        HeartbeatResponse(expiredShuffleKeys, pbHeartbeatResponse.getRegistered)

      case REGISTER_SHUFFLE =>
        val pbRegisterShuffle = PbRegisterShuffle.parseFrom(message.getPayload)
        RegisterShuffle(
          pbRegisterShuffle.getApplicationId,
          pbRegisterShuffle.getShuffleId,
          pbRegisterShuffle.getNumMapppers,
          pbRegisterShuffle.getNumPartitions)

      case REGISTER_SHUFFLE_RESPONSE =>
        val pbRegisterShuffleResponse = PbRegisterShuffleResponse.parseFrom(message.getPayload)
        val partitionLocations = new util.ArrayList[PartitionLocation]()
        if (pbRegisterShuffleResponse.getPartitionLocationsCount > 0) {
          partitionLocations.addAll(pbRegisterShuffleResponse.getPartitionLocationsList
            .asScala.map(PartitionLocation.fromPbPartitionLocation).toList.asJava)
        }
        RegisterShuffleResponse(
          Utils.toStatusCode(pbRegisterShuffleResponse.getStatus),
          partitionLocations)

      case REQUEST_SLOTS =>
        val pbRequestSlots = PbRequestSlots.parseFrom(message.getPayload)
        val userIdentifier = UserIdentifier(
          pbRequestSlots.getUserIdentifier.getTenantId,
          pbRequestSlots.getUserIdentifier.getName)
        RequestSlots(
          pbRequestSlots.getApplicationId,
          pbRequestSlots.getShuffleId,
          new util.ArrayList[Integer](pbRequestSlots.getPartitionIdListList),
          pbRequestSlots.getHostname,
          pbRequestSlots.getShouldReplicate,
          userIdentifier,
          pbRequestSlots.getRequestId)

      case RELEASE_SLOTS =>
        val pbReleaseSlots = PbReleaseSlots.parseFrom(message.getPayload)
        val slotsList = pbReleaseSlots.getSlotsList.asScala.map(pbSlot =>
          new util.HashMap[String, Integer](pbSlot.getSlotMap)).toList.asJava
        ReleaseSlots(
          pbReleaseSlots.getApplicationId,
          pbReleaseSlots.getShuffleId,
          new util.ArrayList[String](pbReleaseSlots.getWorkerIdsList),
          new util.ArrayList[util.Map[String, Integer]](slotsList),
          pbReleaseSlots.getRequestId)

      case RELEASE_SLOTS_RESPONSE =>
        val pbReleaseSlotsResponse = PbReleaseSlotsResponse.parseFrom(message.getPayload)
        ReleaseSlotsResponse(Utils.toStatusCode(pbReleaseSlotsResponse.getStatus))

      case REQUEST_SLOTS_RESPONSE =>
        val pbRequestSlotsResponse = PbRequestSlotsResponse.parseFrom(message.getPayload)
        RequestSlotsResponse(
          Utils.toStatusCode(pbRequestSlotsResponse.getStatus),
          Utils.convertPbWorkerResourceToWorkerResource(
            pbRequestSlotsResponse.getWorkerResourceMap))

      case REVIVE =>
        val pbRevive = PbRevive.parseFrom(message.getPayload)
        val oldPartition =
          if (pbRevive.hasOldPartition) {
            PartitionLocation.fromPbPartitionLocation(pbRevive.getOldPartition)
          } else {
            null
          }
        Revive(
          pbRevive.getApplicationId,
          pbRevive.getShuffleId,
          pbRevive.getMapId,
          pbRevive.getAttemptId,
          pbRevive.getPartitionId,
          pbRevive.getEpoch,
          oldPartition,
          Utils.toStatusCode(pbRevive.getStatus))

      case CHANGE_LOCATION_RESPONSE =>
        val pbReviveResponse = PbChangeLocationResponse.parseFrom(message.getPayload)
        val loc =
          if (pbReviveResponse.hasLocation) {
            PartitionLocation.fromPbPartitionLocation(pbReviveResponse.getLocation)
          } else null
        ChangeLocationResponse(Utils.toStatusCode(pbReviveResponse.getStatus), loc)

      case MAPPER_END =>
        val pbMapperEnd = PbMapperEnd.parseFrom(message.getPayload)
        MapperEnd(
          pbMapperEnd.getApplicationId,
          pbMapperEnd.getShuffleId,
          pbMapperEnd.getMapId,
          pbMapperEnd.getAttemptId,
          pbMapperEnd.getNumMappers)

      case MAPPER_END_RESPONSE =>
        val pbMapperEndResponse = PbMapperEndResponse.parseFrom(message.getPayload)
        MapperEndResponse(Utils.toStatusCode(pbMapperEndResponse.getStatus))

      case GET_REDUCER_FILE_GROUP =>
        val pbGetReducerFileGroup = PbGetReducerFileGroup.parseFrom(message.getPayload)
        GetReducerFileGroup(
          pbGetReducerFileGroup.getApplicationId,
          pbGetReducerFileGroup.getShuffleId)

      case GET_REDUCER_FILE_GROUP_RESPONSE =>
        val pbGetReducerFileGroupResponse = PbGetReducerFileGroupResponse
          .parseFrom(message.getPayload)
        val fileGroup = pbGetReducerFileGroupResponse.getFileGroupList.asScala.map { fg =>
          fg.getLocationsList.asScala.map(PartitionLocation.fromPbPartitionLocation).toArray
        }.toArray
        val attempts = pbGetReducerFileGroupResponse.getAttemptsList.asScala.map(_.toInt).toArray
        GetReducerFileGroupResponse(
          Utils.toStatusCode(pbGetReducerFileGroupResponse.getStatus),
          fileGroup,
          attempts)

      case UNREGISTER_SHUFFLE =>
        val pbUnregisterShuffle = PbUnregisterShuffle.parseFrom(message.getPayload)
        UnregisterShuffle(
          pbUnregisterShuffle.getAppId,
          pbUnregisterShuffle.getShuffleId,
          pbUnregisterShuffle.getRequestId)

      case UNREGISTER_SHUFFLE_RESPONSE =>
        val pbUnregisterShuffleResponse = PbUnregisterShuffleResponse.parseFrom(message.getPayload)
        UnregisterShuffleResponse(Utils.toStatusCode(pbUnregisterShuffleResponse.getStatus))

      case APPLICATION_LOST =>
        val pbApplicationLost = PbApplicationLost.parseFrom(message.getPayload)
        ApplicationLost(pbApplicationLost.getAppId, pbApplicationLost.getRequestId)

      case APPLICATION_LOST_RESPONSE =>
        val pbApplicationLostResponse = PbApplicationLostResponse.parseFrom(message.getPayload)
        ApplicationLostResponse(Utils.toStatusCode(pbApplicationLostResponse.getStatus))

      case HEARTBEAT_FROM_APPLICATION =>
        val pbHeartbeatFromApplication = PbHeartbeatFromApplication.parseFrom(message.getPayload)
        HeartbeatFromApplication(
          pbHeartbeatFromApplication.getAppId,
          pbHeartbeatFromApplication.getTotalWritten,
          pbHeartbeatFromApplication.getFileCount,
          pbHeartbeatFromApplication.getRequestId)

      case GET_BLACKLIST =>
        val pbGetBlacklist = PbGetBlacklist.parseFrom(message.getPayload)
        GetBlacklist(new util.ArrayList[WorkerInfo](pbGetBlacklist.getLocalBlackListList.asScala
          .map(WorkerInfo.fromPbWorkerInfo).toList.asJava))

      case GET_BLACKLIST_RESPONSE =>
        val pbGetBlacklistResponse = PbGetBlacklistResponse.parseFrom(message.getPayload)
        GetBlacklistResponse(
          Utils.toStatusCode(pbGetBlacklistResponse.getStatus),
          pbGetBlacklistResponse.getBlacklistList.asScala
            .map(WorkerInfo.fromPbWorkerInfo).toList.asJava,
          pbGetBlacklistResponse.getUnknownWorkersList.asScala
            .map(WorkerInfo.fromPbWorkerInfo).toList.asJava)

      case CHECK_QUOTA =>
        val pbCheckAvailable = PbCheckQuota.parseFrom(message.getPayload)
        CheckQuota(
          UserIdentifier(
            pbCheckAvailable.getUserIdentifier.getTenantId,
            pbCheckAvailable.getUserIdentifier.getName))

      case CHECK_QUOTA_RESPONSE =>
        val pbCheckAvailableResponse = PbCheckQuotaResponse
          .parseFrom(message.getPayload)
        CheckQuotaResponse(pbCheckAvailableResponse.getAvailable)

      case REPORT_WORKER_FAILURE =>
        val pbReportWorkerFailure = PbReportWorkerFailure.parseFrom(message.getPayload)
        ReportWorkerFailure(
          new util.ArrayList[WorkerInfo](pbReportWorkerFailure.getFailedList
            .asScala.map(WorkerInfo.fromPbWorkerInfo(_)).toList.asJava),
          pbReportWorkerFailure.getRequestId)

      case REGISTER_WORKER_RESPONSE =>
        PbRegisterWorkerResponse.parseFrom(message.getPayload)

      case REREGISTER_WORKER_RESPONSE =>
        val pbReregisterWorkerResponse = PbReregisterWorkerResponse.parseFrom(message.getPayload)
        ReregisterWorkerResponse(pbReregisterWorkerResponse.getSuccess)

      case RESERVE_SLOTS =>
        val pbReserveSlots = PbReserveSlots.parseFrom(message.getPayload)
        val userIdentifier = UserIdentifier(
          pbReserveSlots.getUserIdentifier.getTenantId,
          pbReserveSlots.getUserIdentifier.getName)
        ReserveSlots(
          pbReserveSlots.getApplicationId,
          pbReserveSlots.getShuffleId,
          new util.ArrayList[PartitionLocation](pbReserveSlots.getMasterLocationsList.asScala
            .map(PartitionLocation.fromPbPartitionLocation(_)).toList.asJava),
          new util.ArrayList[PartitionLocation](pbReserveSlots.getSlaveLocationsList.asScala
            .map(PartitionLocation.fromPbPartitionLocation(_)).toList.asJava),
          pbReserveSlots.getSplitThreshold,
          Utils.toShuffleSplitMode(pbReserveSlots.getSplitMode),
          Utils.toPartitionType(pbReserveSlots.getPartitionType),
          pbReserveSlots.getRangeReadFilter,
          userIdentifier)

      case RESERVE_SLOTS_RESPONSE =>
        val pbReserveSlotsResponse = PbReserveSlotsResponse.parseFrom(message.getPayload)
        ReserveSlotsResponse(
          Utils.toStatusCode(pbReserveSlotsResponse.getStatus),
          pbReserveSlotsResponse.getReason)

      case COMMIT_FILES =>
        val pbCommitFiles = PbCommitFiles.parseFrom(message.getPayload)
        CommitFiles(
          pbCommitFiles.getApplicationId,
          pbCommitFiles.getShuffleId,
          pbCommitFiles.getMasterIdsList,
          pbCommitFiles.getSlaveIdsList,
          pbCommitFiles.getMapAttemptsList.asScala.map(_.toInt).toArray)

      case COMMIT_FILES_RESPONSE =>
        val pbCommitFilesResponse = PbCommitFilesResponse.parseFrom(message.getPayload)
        val committedMasterStorageInfos = new util.HashMap[String, StorageInfo]()
        val committedSlaveStorageInfos = new util.HashMap[String, StorageInfo]()
        val committedBitMap = new util.HashMap[String, RoaringBitmap]()
        pbCommitFilesResponse.getCommittedMasterStorageInfosMap.asScala.foreach(entry =>
          committedMasterStorageInfos.put(entry._1, StorageInfo.fromPb(entry._2)))
        pbCommitFilesResponse.getCommittedSlaveStorageInfosMap.asScala.foreach(entry =>
          committedSlaveStorageInfos.put(entry._1, StorageInfo.fromPb(entry._2)))
        pbCommitFilesResponse.getMapIdBitmapMap.asScala.foreach { entry =>
          committedBitMap.put(entry._1, Utils.byteStringToRoaringBitmap(entry._2))
        }
        CommitFilesResponse(
          Utils.toStatusCode(pbCommitFilesResponse.getStatus),
          pbCommitFilesResponse.getCommittedMasterIdsList,
          pbCommitFilesResponse.getCommittedSlaveIdsList,
          pbCommitFilesResponse.getFailedMasterIdsList,
          pbCommitFilesResponse.getFailedSlaveIdsList,
          committedMasterStorageInfos,
          committedSlaveStorageInfos,
          committedBitMap,
          pbCommitFilesResponse.getTotalWritten,
          pbCommitFilesResponse.getFileCount)

      case DESTROY =>
        val pbDestroy = PbDestroy.parseFrom(message.getPayload)
        Destroy(
          pbDestroy.getShuffleKey,
          pbDestroy.getMasterLocationsList,
          pbDestroy.getSlaveLocationList)

      case DESTROY_RESPONSE =>
        val pbDestroyResponse = PbDestroyResponse.parseFrom(message.getPayload)
        DestroyResponse(
          Utils.toStatusCode(pbDestroyResponse.getStatus),
          pbDestroyResponse.getFailedMastersList,
          pbDestroyResponse.getFailedSlavesList)

      case SLAVE_LOST_RESPONSE =>
        val pbSlaveLostResponse = PbSlaveLostResponse.parseFrom(message.getPayload)
        SlaveLostResponse(
          Utils.toStatusCode(pbSlaveLostResponse.getStatus),
          PartitionLocation.fromPbPartitionLocation(pbSlaveLostResponse.getSlaveLocation))

      case GET_WORKER_INFO =>
        GetWorkerInfos

      case GET_WORKER_INFO_RESPONSE =>
        val pbGetWorkerInfoResponse = PbGetWorkerInfosResponse.parseFrom(message.getPayload)
        GetWorkerInfosResponse(
          Utils.toStatusCode(pbGetWorkerInfoResponse.getStatus),
          pbGetWorkerInfoResponse.getWorkerInfosList.asScala
            .map(WorkerInfo.fromPbWorkerInfo(_)).toList: _*)

      case THREAD_DUMP =>
        ThreadDump

      case THREAD_DUMP_RESPONSE =>
        val pbThreadDumpResponse = PbThreadDumpResponse.parseFrom(message.getPayload)
        ThreadDumpResponse(pbThreadDumpResponse.getThreadDump)

      case REMOVE_EXPIRED_SHUFFLE =>
        RemoveExpiredShuffle

      case ONE_WAY_MESSAGE_RESPONSE =>
        OneWayMessageResponse

      case CHECK_FOR_WORKER_TIMEOUT =>
        pbCheckForWorkerTimeout

      case CHECK_FOR_APPLICATION_TIMEOUT =>
        CheckForApplicationTimeOut

      case WORKER_LOST =>
        val pbWorkerLost = PbWorkerLost.parseFrom(message.getPayload)
        WorkerLost(
          pbWorkerLost.getHost,
          pbWorkerLost.getRpcPort,
          pbWorkerLost.getPushPort,
          pbWorkerLost.getFetchPort,
          pbWorkerLost.getReplicatePort,
          pbWorkerLost.getRequestId)

      case WORKER_LOST_RESPONSE =>
        val pbWorkerLostResponse = PbWorkerLostResponse.parseFrom(message.getPayload)
        WorkerLostResponse(pbWorkerLostResponse.getSuccess)

      case STAGE_END =>
        val pbStageEnd = PbStageEnd.parseFrom(message.getPayload)
        StageEnd(pbStageEnd.getApplicationId, pbStageEnd.getShuffleId)

      case PARTITION_SPLIT =>
        val pbShuffleSplitRequest = PbPartitionSplit.parseFrom(message.getPayload)
        val partition =
          if (pbShuffleSplitRequest.hasOldPartition) {
            PartitionLocation.fromPbPartitionLocation(pbShuffleSplitRequest.getOldPartition)
          } else {
            null
          }
        PartitionSplit(
          pbShuffleSplitRequest.getApplicationId,
          pbShuffleSplitRequest.getShuffleId,
          pbShuffleSplitRequest.getPartitionId,
          pbShuffleSplitRequest.getEpoch,
          partition)

      case STAGE_END_RESPONSE =>
        val pbStageEndResponse = PbStageEndResponse.parseFrom(message.getPayload)
        StageEndResponse(Utils.toStatusCode(pbStageEndResponse.getStatus))
    }
  }
}
