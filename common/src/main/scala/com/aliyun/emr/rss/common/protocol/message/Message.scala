package com.aliyun.emr.rss.common.protocol.message

import scala.collection.JavaConverters._

import com.aliyun.emr.rss.common.meta.WorkerInfo
import com.aliyun.emr.rss.common.network.protocol.TransportMessage
import com.aliyun.emr.rss.common.protocol._
import com.aliyun.emr.rss.common.protocol.message.ControlMessages._
import com.aliyun.emr.rss.common.util.Utils

trait Message extends Serializable {
  def toTransportMessage: TransportMessage = {
    this match {
      case CheckForApplicationTimeOut =>
        new TransportMessage(MessageType.CHECK_FOR_APPLICATION_TIMEOUT, null)

      case RemoveExpiredShuffle =>
        new TransportMessage(MessageType.REMOVE_EXPIRED_SHUFFLE, null)

      case RegisterWorker(host, rpcPort, pushPort, fetchPort, replicatePort, disks, requestId) =>
        val pbDisks = disks.asScala
          .map(item =>
            item._1 -> PbDiskInfo
              .newBuilder()
              .setUsableSpace(item._2.actualUsableSpace)
              .setAvgFlushTime(item._2.avgFlushTime)
              .setUsedSlots(item._2.activeSlots)
              .build()).toMap.asJava
        val payload = PbRegisterWorker.newBuilder()
          .setHost(host)
          .setRpcPort(rpcPort)
          .setPushPort(pushPort)
          .setFetchPort(fetchPort)
          .setReplicatePort(replicatePort)
          .putAllDisks(pbDisks)
          .setRequestId(requestId)
          .build().toByteArray
        new TransportMessage(MessageType.REGISTER_WORKER, payload)

      case HeartbeatFromWorker(
            host,
            rpcPort,
            pushPort,
            fetchPort,
            replicatePort,
            disks,
            shuffleKeys,
            requestId) =>
        val pbDisks = disks.asScala
          .map(item =>
            item._1 -> PbDiskInfo
              .newBuilder()
              .setUsableSpace(item._2.actualUsableSpace)
              .setAvgFlushTime(item._2.avgFlushTime)
              .setUsedSlots(item._2.activeSlots)
              .setStatus(item._2.status.getValue)
              .build()).toMap.asJava
        val payload = PbHeartbeatFromWorker.newBuilder()
          .setHost(host)
          .setRpcPort(rpcPort)
          .setPushPort(pushPort)
          .setFetchPort(fetchPort)
          .putAllDisks(pbDisks)
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
            requestId) =>
        val payload = PbRequestSlots.newBuilder()
          .setApplicationId(applicationId)
          .setShuffleId(shuffleId)
          .addAllPartitionIdList(partitionIdList)
          .setHostname(hostname)
          .setShouldReplicate(shouldReplicate)
          .setRequestId(requestId)
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

      case RegisterWorkerResponse(success, message) =>
        val payload = PbRegisterWorkerResponse.newBuilder()
          .setSuccess(success)
          .setMessage(message)
          .build().toByteArray
        new TransportMessage(MessageType.REGISTER_WORKER_RESPONSE, payload)

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
            rangeReadFilter) =>
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
  }
}

trait MasterMessage extends Message

abstract class MasterRequestMessage extends MasterMessage {
  var requestId: String

  def requestId_(id: String): Unit = {
    this.requestId = id
  }
}

trait WorkerMessage extends Message

trait ClientMessage extends Message
