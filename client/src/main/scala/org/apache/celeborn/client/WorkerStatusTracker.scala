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

import java.util.{HashSet => JHashSet, List => JList, Set => JSet}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.celeborn.client.LifecycleManager.ShuffleFailedWorkers
import org.apache.celeborn.client.listener.{WorkersStatus, WorkerStatusListener}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.ControlMessages.HeartbeatFromApplicationResponse
import org.apache.celeborn.common.protocol.message.StatusCode

class WorkerStatusTracker(
    conf: CelebornConf,
    lifecycleManager: LifecycleManager) extends Logging {
  private val excludedWorkerExpireTimeout = conf.clientExcludedWorkerExpireTimeout
  private val workerStatusListeners = ConcurrentHashMap.newKeySet[WorkerStatusListener]()

  // blacklist
  val blacklist = new ShuffleFailedWorkers()
  val shuttingWorkers: JSet[WorkerInfo] = new JHashSet[WorkerInfo]()

  def registerWorkerStatusListener(workerStatusListener: WorkerStatusListener): Unit = {
    workerStatusListeners.add(workerStatusListener)
  }

  def getNeedCheckedWorkers(): Set[WorkerInfo] = {
    if (conf.clientCheckedUseAllocatedWorkers) {
      lifecycleManager.getAllocatedWorkers()
    } else {
      blacklist.asScala.keys.toSet ++ shuttingWorkers.asScala.toSet
    }
  }

  def workerAvailable(worker: WorkerInfo): Boolean = {
    !blacklist.containsKey(worker) && !shuttingWorkers.contains(worker)
  }

  def workerAvailable(loc: PartitionLocation): Boolean = {
    if (loc == null) {
      false
    } else {
      workerAvailable(loc.getWorker)
    }
  }

  def blacklistWorkerFromPartition(
      shuffleId: Int,
      oldPartition: PartitionLocation,
      cause: StatusCode): Unit = {
    val failedWorker = new ShuffleFailedWorkers()

    def blacklistWorker(partition: PartitionLocation, statusCode: StatusCode): Unit = {
      val tmpWorker = partition.getWorker
      val worker =
        lifecycleManager.workerSnapshots(shuffleId).keySet().asScala.find(_.equals(tmpWorker))
      if (worker.isDefined) {
        failedWorker.put(worker.get, (statusCode, System.currentTimeMillis()))
      }
    }

    if (oldPartition != null) {
      cause match {
        case StatusCode.PUSH_DATA_WRITE_FAIL_MASTER =>
          blacklistWorker(oldPartition, StatusCode.PUSH_DATA_WRITE_FAIL_MASTER)
        case StatusCode.PUSH_DATA_WRITE_FAIL_SLAVE
            if oldPartition.hasPeer && conf.clientBlacklistSlaveEnabled =>
          blacklistWorker(oldPartition.getPeer, StatusCode.PUSH_DATA_WRITE_FAIL_SLAVE)
        case StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER =>
          blacklistWorker(oldPartition, StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER)
        case StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_SLAVE
            if oldPartition.hasPeer && conf.clientBlacklistSlaveEnabled =>
          blacklistWorker(
            oldPartition.getPeer,
            StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_SLAVE)
        case StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER =>
          blacklistWorker(oldPartition, StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER)
        case StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_SLAVE
            if oldPartition.hasPeer && conf.clientBlacklistSlaveEnabled =>
          blacklistWorker(
            oldPartition.getPeer,
            StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_SLAVE)
        case StatusCode.PUSH_DATA_TIMEOUT_MASTER =>
          blacklistWorker(oldPartition, StatusCode.PUSH_DATA_TIMEOUT_MASTER)
        case StatusCode.PUSH_DATA_TIMEOUT_SLAVE
            if oldPartition.hasPeer && conf.clientBlacklistSlaveEnabled =>
          blacklistWorker(
            oldPartition.getPeer,
            StatusCode.PUSH_DATA_TIMEOUT_SLAVE)
        case _ =>
      }
    }
    recordWorkerFailure(failedWorker)
  }

  def recordWorkerFailure(failures: ShuffleFailedWorkers): Unit = {
    if (!failures.isEmpty) {
      val failedWorker = new ShuffleFailedWorkers(failures)
      val failedWorkerMsg = failedWorker.asScala.map { case (worker, (status, time)) =>
        s"${worker.readableAddress()}   ${status.name()}   $time"
      }.mkString("\n")
      val blacklistMsg = blacklist.asScala.map { case (worker, (status, time)) =>
        s"${worker.readableAddress()}   ${status.name()}   $time"
      }.mkString("\n")
      val shuttingDownMsg = shuttingWorkers.asScala.map(_.readableAddress()).mkString("\n")
      logInfo(
        s"""
           |Reporting Worker Failure:
           |$failedWorkerMsg
           |Current blacklist:
           |$blacklistMsg
           |Current shutting down:
           |$shuttingDownMsg""".stripMargin)
      failedWorker.asScala.foreach {
        case (worker, (StatusCode.WORKER_SHUTDOWN, _)) =>
          shuttingWorkers.add(worker)
        case (worker, (statusCode, registerTime)) if !blacklist.containsKey(worker) =>
          blacklist.put(worker, (statusCode, registerTime))
        case (worker, (statusCode, _))
            if statusCode == StatusCode.NO_AVAILABLE_WORKING_DIR ||
              statusCode == StatusCode.RESERVE_SLOTS_FAILED ||
              statusCode == StatusCode.UNKNOWN_WORKER =>
          blacklist.put(worker, (statusCode, blacklist.get(worker)._2))
        case _ => // Not cover
      }
    }
  }

  def removeFromBlacklist(workers: JHashSet[WorkerInfo]): Unit = {
    blacklist.keySet.removeAll(workers)
  }

  def handleHeartbeatResponse(res: HeartbeatFromApplicationResponse): Unit = {
    if (res.statusCode == StatusCode.SUCCESS) {
      logInfo(s"Received Blacklist from Master, blacklist: ${res.blacklist} " +
        s"unknown workers: ${res.unknownWorkers}, shutdown workers: ${res.shuttingWorkers}")
      val current = System.currentTimeMillis()

      blacklist.asScala.foreach {
        case (workerInfo: WorkerInfo, (statusCode, registerTime)) =>
          statusCode match {
            case StatusCode.UNKNOWN_WORKER |
                StatusCode.NO_AVAILABLE_WORKING_DIR |
                StatusCode.RESERVE_SLOTS_FAILED |
                StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER |
                StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_SLAVE |
                StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER |
                StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_SLAVE |
                StatusCode.PUSH_DATA_TIMEOUT_MASTER |
                StatusCode.PUSH_DATA_TIMEOUT_SLAVE
                if current - registerTime < excludedWorkerExpireTimeout => // reserve
            case _ =>
              if (!res.blacklist.contains(workerInfo) &&
                !res.shuttingWorkers.contains(workerInfo) &&
                !res.unknownWorkers.contains(workerInfo)) {
                blacklist.remove(workerInfo)
              }
          }
      }

      if (!res.blacklist.isEmpty) {
        blacklist.putAll(res.blacklist.asScala.filterNot(blacklist.containsKey)
          .map(_ -> (StatusCode.WORKER_IN_BLACKLIST -> current)).toMap.asJava)
      }

      shuttingWorkers.retainAll(res.shuttingWorkers)
      shuttingWorkers.addAll(res.shuttingWorkers)
      if (!res.unknownWorkers.isEmpty || !res.shuttingWorkers.isEmpty) {
        blacklist.putAll(res.unknownWorkers.asScala.filterNot(blacklist.containsKey)
          .map(_ -> (StatusCode.UNKNOWN_WORKER -> current)).toMap.asJava)
        val workerStatus = new WorkersStatus(res.unknownWorkers, res.shuttingWorkers)
        workerStatusListeners.asScala.foreach { listener =>
          try {
            listener.notifyChangedWorkersStatus(workerStatus)
          } catch {
            case t: Throwable =>
              logError("Error while notify listener", t)
          }
        }
      }

      logInfo(s"Current blacklist $blacklist, Current shuttingDown ${shuttingWorkers.asScala.map(
        _.readableAddress()).mkString("\n")}")
    }
  }
}
