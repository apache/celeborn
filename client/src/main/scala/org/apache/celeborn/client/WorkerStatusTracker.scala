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

import java.util.{function, HashSet => JHashSet, List => JList, Set => JSet}

import scala.collection.JavaConverters._

import org.apache.celeborn.client.LifecycleManager.ShuffleFailedWorkers
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.ControlMessages.HeartbeatFromApplicationResponse
import org.apache.celeborn.common.protocol.message.StatusCode

class WorkerStatusTracker(
    conf: CelebornConf,
    lifecycleManager: LifecycleManager,
    commitManager: CommitManager) extends Logging {
  private val workerExcludedExpireTimeout = conf.workerExcludedExpireTimeout

  // blacklist
  val blacklist = new ShuffleFailedWorkers()
  private val shuttingWorkers: JSet[WorkerInfo] = new JHashSet[WorkerInfo]()

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
            if oldPartition.getPeer != null && conf.blacklistSlaveEnabled =>
          blacklistWorker(oldPartition.getPeer, StatusCode.PUSH_DATA_WRITE_FAIL_SLAVE)
        case StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER =>
          blacklistWorker(oldPartition, StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER)
        case StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_SLAVE
            if oldPartition.getPeer != null && conf.blacklistSlaveEnabled =>
          blacklistWorker(
            oldPartition.getPeer,
            StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_SLAVE)
        case StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER =>
          blacklistWorker(oldPartition, StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER)
        case StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_SLAVE
            if oldPartition.getPeer != null && conf.blacklistSlaveEnabled =>
          blacklistWorker(
            oldPartition.getPeer,
            StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_SLAVE)
        case StatusCode.PUSH_DATA_TIMEOUT_MASTER =>
          blacklistWorker(oldPartition, StatusCode.PUSH_DATA_TIMEOUT_MASTER)
        case StatusCode.PUSH_DATA_TIMEOUT_SLAVE
            if oldPartition.getPeer != null && conf.blacklistSlaveEnabled =>
          blacklistWorker(
            oldPartition.getPeer,
            StatusCode.PUSH_DATA_TIMEOUT_SLAVE)
        case _ =>
      }
    }
    if (!failedWorker.isEmpty) {
      recordWorkerFailure(failedWorker)
    }
  }

  def recordWorkerFailure(failures: ShuffleFailedWorkers): Unit = {
    val failedWorker = new ShuffleFailedWorkers(failures)
    logInfo(s"Report Worker Failure: ${failedWorker.asScala}, current blacklist $blacklist")
    failedWorker.asScala.foreach { case (worker, (statusCode, registerTime)) =>
      if (!blacklist.containsKey(worker)) {
        blacklist.put(worker, (statusCode, registerTime))
      } else {
        statusCode match {
          case StatusCode.WORKER_SHUTDOWN |
              StatusCode.NO_AVAILABLE_WORKING_DIR |
              StatusCode.RESERVE_SLOTS_FAILED |
              StatusCode.UNKNOWN_WORKER =>
            blacklist.put(worker, (statusCode, blacklist.get(worker)._2))
          case _ => // Not cover
        }
      }
    }
  }

  def handleHeartbeatResponse(res: HeartbeatFromApplicationResponse): Unit = {
    if (res.statusCode == StatusCode.SUCCESS) {
      logInfo(s"Received Blacklist from Master, blacklist: ${res.blacklist} " +
        s"unknown workers: ${res.unknownWorkers}, shutdown workers: ${res.shuttingWorkers}")
      resolveShutdownWorkers(res.shuttingWorkers)
      val current = System.currentTimeMillis()
      val reserved = blacklist.asScala
        .filter { case (_, entry) =>
          val (statusCode, registerTime) = entry
          statusCode match {
            case StatusCode.WORKER_SHUTDOWN |
                StatusCode.NO_AVAILABLE_WORKING_DIR |
                StatusCode.RESERVE_SLOTS_FAILED |
                StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER |
                StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_SLAVE |
                StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER |
                StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_SLAVE |
                StatusCode.PUSH_DATA_TIMEOUT_MASTER |
                StatusCode.PUSH_DATA_TIMEOUT_SLAVE
                if current - registerTime < workerExcludedExpireTimeout =>
              true
            case StatusCode.UNKNOWN_WORKER => true
            case _ => false
          }
        }.asJava
      val reservedBlackList = new ShuffleFailedWorkers()
      reservedBlackList.putAll(reserved)
      blacklist.clear()
      blacklist.putAll(
        res.blacklist.asScala.map(_ -> (StatusCode.WORKER_IN_BLACKLIST -> current)).toMap.asJava)
      blacklist.putAll(
        res.unknownWorkers.asScala.map(_ -> (StatusCode.UNKNOWN_WORKER -> current)).toMap.asJava)
      // put reserved blacklist at last to cover blacklist's local status.
      blacklist.putAll(reservedBlackList)
    }
  }

  private def resolveShutdownWorkers(newShutdownWorkers: java.util.List[WorkerInfo]): Unit = {
    // shutdownWorkers only retain workers appeared in response.
    shuttingWorkers.retainAll(newShutdownWorkers)
    newShutdownWorkers.asScala.filterNot(shuttingWorkers.asScala.contains)
      .foreach { workerInfo =>
        commitManager.handleShutdownWorker(workerInfo)
      }
    shuttingWorkers.addAll(newShutdownWorkers)
  }
}
