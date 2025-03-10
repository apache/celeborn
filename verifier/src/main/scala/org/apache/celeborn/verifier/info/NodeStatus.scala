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

package org.apache.celeborn.verifier.info

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.verifier.conf.VerifierConf

class NodeStatus(
    var cores: Int,
    var masterAlive: Boolean,
    var workerAlive: Boolean,
    var workingDirs: Map[String, WorkingDirStatus]) extends Serializable with Logging {

  def this(values: (Int, Boolean, Boolean, Map[String, WorkingDirStatus])) {
    this(values._1, values._2, values._3, values._4)
  }

  def this() {
    this(0, false, false, Map.empty[String, WorkingDirStatus])
  }

  def checkAndUpdate(uid: String, other: NodeStatus): Unit = {
    if ((this.masterAlive != other.masterAlive || this.workerAlive != other.workerAlive
        || !this.workingDirs.equals(other.workingDirs))
      && this.workingDirs.equals(other.workingDirs)) {
      logInfo(
        s"$uid resource stat mismatch, current status: ${this.toString}, other status: ${other.toString}.")
    }
  }

  override def toString: String = s"cores: $cores, " +
    s"masterAlive: $masterAlive, " +
    s"workerAlive: $workerAlive, " +
    s"workingDirs: $workingDirs"

  def except(old: NodeStatus): NodeStatus = {
    new NodeStatus(this.cores - old.cores, old.masterAlive, old.workerAlive, old.workingDirs)
  }

  override def equals(other: Any): Boolean = other match {
    case that: NodeStatus =>
      (that canEqual this) &&
        cores == that.cores &&
        masterAlive == that.masterAlive &&
        workerAlive == that.workerAlive &&
        workingDirs == that.workingDirs
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[NodeStatus]

  override def hashCode(): Int = {
    val state = Seq(cores, masterAlive, workerAlive, workingDirs)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  def duplicate(): NodeStatus = {
    new NodeStatus(
      this.cores,
      this.masterAlive,
      this.workerAlive,
      workingDirs.map(workingDir =>
        workingDir._1 -> new WorkingDirStatus(
          workingDir._2.unwritable,
          workingDir._2.hanging,
          workingDir._2.deviceName)))
  }
}

object NodeStatus {

  def apply(conf: VerifierConf): NodeStatus = {
    val masterExists =
      if (Utils.runCommand("ps -lef | grep Master | grep -v grep").nonEmpty) {
        true
      } else {
        if (VerifierConf.runnerTestMode(conf)) {
          true
        } else {
          false
        }
      }
    val workerExists =
      if (Utils.runCommand("ps -lef | grep Worker | grep -v grep").nonEmpty) {
        true
      } else {
        if (VerifierConf.runnerTestMode(conf)) {
          true
        } else {
          false
        }
      }
    val workingDirs = conf.celebornConf.workerBaseDirs
    new NodeStatus(
      Runtime.getRuntime.availableProcessors(),
      masterExists,
      workerExists,
      workingDirs.map(workingDir =>
        workingDir._1 -> new WorkingDirStatus(
          false,
          false,
          workingDirDeviceMounts(workingDirs.map(f => f._1).toList)(workingDir._1))).toMap)
  }

  private def workingDirDeviceMounts(workingDirs: List[String]): Map[String, String] = {
    val deviceMounts =
      Utils.runCommand("df -h").split("[\n\r]").tail.map(_.trim.split("[ \t]+"))
        .map(sp => (sp(0), sp(sp.length - 1)))
    workingDirs.map(workingDir =>
      workingDir ->
        deviceMounts.filter(deviceMount => workingDir.startsWith(deviceMount._2)).head._1).toMap
  }
}
