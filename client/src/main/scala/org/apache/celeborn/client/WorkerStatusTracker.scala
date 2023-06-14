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

  val excludedWorkers = new ShuffleFailedWorkers()
  private val shuttingWorkers: JSet[WorkerInfo] = new JHashSet[WorkerInfo]()

  def registerWorkerStatusListener(workerStatusListener: WorkerStatusListener): Unit = {
    workerStatusListeners.add(workerStatusListener)
  }

  def getNeedCheckedWorkers(): Set[WorkerInfo] = {
    if (conf.clientCheckedUseAllocatedWorkers) {
      lifecycleManager.getAllocatedWorkers()
    } else {
      excludedWorkers.asScala.keys.toSet
    }
  }

  def excludeWorkerFromPartition(
      shuffleId: Int,
      oldPartition: PartitionLocation,
      cause: StatusCode): Unit = {
    val failedWorker = new ShuffleFailedWorkers()

    def excludeWorker(partition: PartitionLocation, statusCode: StatusCode): Unit = {
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
          excludeWorker(oldPartition, StatusCode.PUSH_DATA_WRITE_FAIL_MASTER)
        case StatusCode.PUSH_DATA_WRITE_FAIL_SLAVE
            if oldPartition.hasPeer && conf.clientExcludeSlaveOnFailureEnabled =>
          blacklistWorker(oldPartition.getPeer, StatusCode.PUSH_DATA_WRITE_FAIL_SLAVE)
        case StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER =>
          excludeWorker(oldPartition, StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER)
        case StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_SLAVE
            if oldPartition.hasPeer && conf.clientExcludeSlaveOnFailureEnabled =>
          blacklistWorker(
            oldPartition.getPeer,
            StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_SLAVE)
        case StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER =>
          excludeWorker(oldPartition, StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER)
        case StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_SLAVE
            if oldPartition.hasPeer && conf.clientExcludeSlaveOnFailureEnabled =>
          blacklistWorker(
            oldPartition.getPeer,
            StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_SLAVE)
        case StatusCode.PUSH_DATA_TIMEOUT_MASTER =>
          excludeWorker(oldPartition, StatusCode.PUSH_DATA_TIMEOUT_MASTER)
        case StatusCode.PUSH_DATA_TIMEOUT_SLAVE
            if oldPartition.hasPeer && conf.clientExcludeSlaveOnFailureEnabled =>
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
      val excludedWorkersMsg = excludedWorkers.asScala.map { case (worker, (status, time)) =>
        s"${worker.readableAddress()}   ${status.name()}   $time"
      }.mkString("\n")
      logInfo(
        s"""
           |Reporting Worker Failure:
           |$failedWorkerMsg
           |Current excluded workers:
           |$excludedWorkersMsg
               """.stripMargin)
      failedWorker.asScala.foreach { case (worker, (statusCode, registerTime)) =>
        if (!excludedWorkers.containsKey(worker)) {
          excludedWorkers.put(worker, (statusCode, registerTime))
        } else {
          statusCode match {
            case StatusCode.WORKER_SHUTDOWN |
                StatusCode.NO_AVAILABLE_WORKING_DIR |
                StatusCode.RESERVE_SLOTS_FAILED |
                StatusCode.UNKNOWN_WORKER =>
              excludedWorkers.put(worker, (statusCode, excludedWorkers.get(worker)._2))
            case _ => // Not cover
          }
        }
      }
    }
  }

  def removeFromExcludedList(workers: JHashSet[WorkerInfo]): Unit = {
    excludedWorkers.keySet.removeAll(workers)
  }

  def handleHeartbeatResponse(res: HeartbeatFromApplicationResponse): Unit = {
    if (res.statusCode == StatusCode.SUCCESS) {
      logDebug(s"Received excluded worker from Master, excluded workers: ${res.excludedWorkers} " +
        s"unknown workers: ${res.unknownWorkers}, shutdown workers: ${res.shuttingWorkers}")
      val current = System.currentTimeMillis()

      excludedWorkers.asScala.foreach {
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
              if (!res.excludedWorkers.contains(workerInfo) &&
                !res.shuttingWorkers.contains(workerInfo) &&
                !res.unknownWorkers.contains(workerInfo)) {
                excludedWorkers.remove(workerInfo)
              }
          }
      }

      if (!res.excludedWorkers.isEmpty) {
        excludedWorkers.putAll(res.excludedWorkers.asScala.filterNot(excludedWorkers.containsKey)
          .map(_ -> (StatusCode.WORKER_EXCLUDED -> current)).toMap.asJava)
      }

      if (!res.shuttingWorkers.isEmpty) {
        excludedWorkers.putAll(res.shuttingWorkers.asScala.filterNot(excludedWorkers.containsKey)
          .map(_ -> (StatusCode.WORKER_SHUTDOWN -> current)).toMap.asJava)
      }

      val newShutdownWorkers = resolveShutdownWorkers(res.shuttingWorkers)
      if (!res.unknownWorkers.isEmpty || !newShutdownWorkers.isEmpty) {
        excludedWorkers.putAll(res.unknownWorkers.asScala.filterNot(excludedWorkers.containsKey)
          .map(_ -> (StatusCode.UNKNOWN_WORKER -> current)).toMap.asJava)
        val workerStatus = new WorkersStatus(res.unknownWorkers, newShutdownWorkers)
        workerStatusListeners.asScala.foreach { listener =>
          try {
            listener.notifyChangedWorkersStatus(workerStatus)
          } catch {
            case t: Throwable =>
              logError("Error while notify listener", t)
          }
        }
      }

      logDebug(s"Current excluded workers $excludedWorkers")
    }
  }

  private def resolveShutdownWorkers(newShutdownWorkers: JList[WorkerInfo]): JList[WorkerInfo] = {
    // shutdownWorkers only retain workers appeared in response.
    shuttingWorkers.retainAll(newShutdownWorkers)
    val shutdownList = newShutdownWorkers.asScala.filterNot(shuttingWorkers.asScala.contains).asJava
    shuttingWorkers.addAll(newShutdownWorkers)
    shutdownList
  }
}
