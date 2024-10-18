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

package org.apache.celeborn.service.deploy.worker

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.immutable.HashSet

import com.google.common.collect.Sets

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerStatus
import org.apache.celeborn.common.protocol.PbWorkerStatus.State
import org.apache.celeborn.common.protocol.WorkerEventType
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.service.deploy.worker.storage.StorageManager

private[celeborn] class WorkerStatusManager(conf: CelebornConf) extends Logging {

  var currentWorkerStatus = WorkerStatus.normalWorkerStatus()
  var exitEventType = WorkerEventType.Immediately
  private var worker: Worker = _
  private var shutdown: AtomicBoolean = _
  private var storageManager: StorageManager = _
  private val gracefulShutdown = conf.workerGracefulShutdown
  if (gracefulShutdown) {
    exitEventType = WorkerEventType.Graceful
  }

  private val transitionStateMap = new util.HashMap[State, util.HashSet[State]]()
  transitionStateMap.put(
    State.Normal,
    Sets.newHashSet(
      State.InGraceFul,
      State.InExit,
      State.InDecommission,
      State.InDecommissionThenIdle))
  transitionStateMap.put(
    State.InDecommissionThenIdle,
    Sets.newHashSet(State.Normal, State.Idle, State.InGraceFul, State.InExit, State.InDecommission))
  transitionStateMap.put(
    State.Idle,
    Sets.newHashSet(State.Normal, State.InGraceFul, State.InExit, State.InDecommission))
  transitionStateMap.put(State.InGraceFul, Sets.newHashSet(State.Exit))
  transitionStateMap.put(State.InDecommission, Sets.newHashSet(State.Exit))
  transitionStateMap.put(State.InExit, Sets.newHashSet(State.Exit))

  private val exitStatus = HashSet(
    State.InExit,
    State.Exit,
    State.InDecommission,
    State.InGraceFul)

  def init(worker: Worker): Unit = {
    this.worker = worker
    shutdown = worker.shutdown
    storageManager = worker.storageManager
  }

  def doTransition(eventType: WorkerEventType): Unit = this.synchronized {
    if (inExitStatus()) {
      logDebug(s"Worker receive event: $eventType, but in exit State: ${getWorkerState()} ")
    } else {
      logDebug(s"Worker receive event: $eventType, currentState: ${getWorkerState()} ")
      checkIfNeedTransitionStatus()
      val currentState = getWorkerState()
      eventType match {
        case WorkerEventType.DecommissionThenIdle if currentState == State.Normal =>
          decommissionWorkerThenIdle()
        case WorkerEventType.Recommission
            if currentState == State.InDecommissionThenIdle || currentState == State.Idle =>
          recommissionWorker()
        case WorkerEventType.Graceful | WorkerEventType.Immediately | WorkerEventType.Decommission =>
          exit(eventType)
        case _ =>
          logDebug(s"Worker receive event: $eventType, and has nothing to do ")
      }
    }
  }

  def checkIfNeedTransitionStatus(): Unit = this.synchronized {
    currentWorkerStatus.getState match {
      case State.InDecommissionThenIdle if decommissionThenIdleFinished() =>
        transitionState(State.Idle)
      case _ =>
    }
  }

  private def exit(eventType: WorkerEventType): Unit = {
    exitEventType = eventType
    exitEventType match {
      case WorkerEventType.Immediately => transitionState(State.InExit)
      case WorkerEventType.Graceful => transitionState(State.InGraceFul)
      case WorkerEventType.Decommission => transitionState(State.InDecommission)
      case _ => // ignore
    }

    // Compatible with current exit logic
    // trigger shutdown hook to exit
    ThreadUtils.newThreadWithDefaultUncaughtExceptionHandler(
      new Runnable {
        override def run(): Unit = {
          Thread.sleep(10000)
          System.exit(0)
        }
      },
      "worker-exit-thread").start()
  }

  def transitionState(state: State): Unit = this.synchronized {
    val allowStates = transitionStateMap.get(currentWorkerStatus.getState)
    if (allowStates != null && allowStates.contains(state)) {
      logInfo(s"Worker transition status from ${currentWorkerStatus.getState} to $state.")
      currentWorkerStatus = new WorkerStatus(state.getNumber, System.currentTimeMillis())
    } else {
      logWarning(
        s"Worker transition status from ${currentWorkerStatus.getState} to $state is not allowed.")
    }
  }

  private def recommissionWorker(): Unit = this.synchronized {
    shutdown.set(false)
    transitionState(State.Normal)
  }

  private def decommissionWorkerThenIdle(): Unit = this.synchronized {
    shutdown.set(true)
    transitionState(State.InDecommissionThenIdle)
    worker.sendWorkerDecommissionToMaster()
    checkIfNeedTransitionStatus()
  }

  private def decommissionThenIdleFinished(): Boolean = this.synchronized {
    shutdown.get() && (storageManager.shuffleKeySet().isEmpty || currentWorkerStatus.getState == State.Idle)
  }

  def getWorkerState(): State = {
    currentWorkerStatus.getState
  }

  private def inExitStatus(): Boolean = {
    exitStatus.contains(currentWorkerStatus.getState)
  }
}
