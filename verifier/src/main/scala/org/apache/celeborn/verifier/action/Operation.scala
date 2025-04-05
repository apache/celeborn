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

package org.apache.celeborn.verifier.action

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.Random

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.verifier.runner.RunnerContext
import org.apache.celeborn.verifier.scheduler.SchedulerContext

abstract class Operation(
    val actionTarget: ActionTarget,
    val updateContextBlock: RunnerContext => Unit = _ => {},
    val interval: Long)
  extends Serializable with Logging {

  def executeOnRunner(context: RunnerContext): OperationResult

  def updateSchedulerContext(context: SchedulerContext): Unit = {
    val runContext =
      RunnerContext(context.conf, context.runnerInfos.get(actionTarget.identity).resource)
    updateContextBlock(runContext)
  }

  def executeCommand(command: String, block: => Unit): OperationResult = {
    try {
      val commandResult = Utils.runCommand(command)
      logDebug(s"Command $command executed, result: $commandResult.")
      block
      new OperationResult(commandResult, null)
    } catch {
      case e: Exception =>
        new OperationResult(OperationResult.OPERATION_FAILURE, e)
    }
  }

  override def toString: String = s"Operation($actionTarget, $interval)"
}

class BashOperation(
    override val actionTarget: ActionTarget,
    val command: String,
    override val updateContextBlock: RunnerContext => Unit = _ => {},
    override val interval: Long) extends Operation(actionTarget, interval = interval) {

  override def executeOnRunner(context: RunnerContext): OperationResult = {
    executeCommand(command, updateContextBlock(context))
  }
}

class OccupyCpuOperation(
    override val actionTarget: ActionTarget,
    val cores: Int,
    val duration: Long,
    override val interval: Long)
  extends Operation(actionTarget, interval = interval) {

  private val MAX_SLEEP_TIME = 5 * 60 * 1000
  private var c: Long = 0

  override def executeOnRunner(context: RunnerContext): OperationResult = {
    try {
      val threadFlag = new AtomicBoolean(true)
      val threadNum = cores
      val threads = (1 to threadNum).map(_ => new Thread(new CpuConsumer(threadFlag))).toList
      threads.foreach(_.start())
      val safetyDuration =
        if (duration > MAX_SLEEP_TIME) {
          MAX_SLEEP_TIME
        } else {
          this.duration
        }
      Thread.sleep(safetyDuration)
      threadFlag.set(false)
      new OperationResult(OperationResult.OPERATION_SUCCESS, null)
    } catch {
      case e: Exception =>
        new OperationResult(OperationResult.OPERATION_FAILURE, e)
    }
  }

  private class CpuConsumer(val flag: AtomicBoolean) extends Runnable {

    override def run(): Unit = {
      val random = Random
      while (flag.get()) {
        val a = random.nextInt()
        val b = random.nextInt()
        if (b != 0) {
          c = a + b * a / b
        } else {
          c = a + b * a
        }
      }
    }
  }
}

class OperationResult(val result: String, val exception: Exception) {}

object OperationResult {
  val OPERATION_SUCCESS: String = "success"
  val OPERATION_FAILURE: String = "failure"
}
