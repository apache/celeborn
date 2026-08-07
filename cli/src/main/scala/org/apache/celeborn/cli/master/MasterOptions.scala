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

package org.apache.celeborn.cli.master

import picocli.CommandLine.Option

final class MasterOptions {

  @Option(names = Array("--show-masters-info"), description = Array("Show master group info"))
  private[master] var showMastersInfo: Boolean = _

  @Option(
    names = Array("--show-cluster-apps"),
    description = Array("Show cluster application's ids"))
  private[master] var showClusterApps: Boolean = _

  @Option(
    names = Array("--show-cluster-apps-info"),
    description = Array("Show cluster application's info"))
  private[master] var showClusterAppsInfo: Boolean = _

  @Option(names = Array("--show-cluster-shuffles"), description = Array("Show cluster shuffles"))
  private[master] var showClusterShuffles: Boolean = _

  @Option(
    names = Array("--unregister-shuffles"),
    description = Array("Unregister shuffles from the service"))
  private[master] var unregisterShuffles: Boolean = _

  @Option(names = Array("--exclude-worker"), description = Array("Exclude workers by ID"))
  private[master] var excludeWorkers: Boolean = _

  @Option(
    names = Array("--remove-excluded-worker"),
    description = Array("Remove excluded workers by ID"))
  private[master] var removeExcludedWorkers: Boolean = _

  @Option(
    names = Array("--send-worker-event"),
    paramLabel =
      "IMMEDIATELY | DECOMMISSION | DECOMMISSION_THEN_IDLE | GRACEFUL | RECOMMISSION | NONE",
    description = Array("Send an event to a worker"))
  private[master] var sendWorkerEvent: String = _

  @Option(
    names = Array("--show-worker-event-info"),
    description = Array("Show worker event information"))
  private[master] var showWorkerEventInfo: Boolean = _

  @Option(names = Array("--show-lost-workers"), description = Array("Show lost workers"))
  private[master] var showLostWorkers: Boolean = _

  @Option(names = Array("--show-excluded-workers"), description = Array("Show excluded workers"))
  private[master] var showExcludedWorkers: Boolean = _

  @Option(
    names = Array("--show-manual-excluded-workers"),
    description = Array("Show manual excluded workers"))
  private[master] var showManualExcludedWorkers: Boolean = _

  @Option(names = Array("--show-shutdown-workers"), description = Array("Show shutdown workers"))
  private[master] var showShutdownWorkers: Boolean = _

  @Option(
    names = Array("--show-decommissioning-workers"),
    description = Array("Show decommissioning workers"))
  private[master] var showDecommissioningWorkers: Boolean = _

  @Option(
    names = Array("--show-lifecycle-managers"),
    description = Array("Show lifecycle managers"))
  private[master] var showLifecycleManagers: Boolean = _

  @Option(names = Array("--show-workers"), description = Array("Show registered workers"))
  private[master] var showWorkers: Boolean = _

  @Option(
    names = Array("--show-workers-topology"),
    description = Array("Show registered workers topology"))
  private[master] var showWorkersTopology: Boolean = _

  @Option(names = Array("--show-conf"), description = Array("Show master conf"))
  private[master] var showConf: Boolean = _

  @Option(names = Array("--show-dynamic-conf"), description = Array("Show dynamic master conf"))
  private[master] var showDynamicConf: Boolean = _

  @Option(names = Array("--upsert-dynamic-conf"), description = Array("Upsert dynamic master conf"))
  private[master] var upsertDynamicConf: Boolean = _

  @Option(names = Array("--delete-dynamic-conf"), description = Array("Delete dynamic master conf"))
  private[master] var deleteDynamicConf: Boolean = _

  @Option(names = Array("--show-thread-dump"), description = Array("Show master thread dump"))
  private[master] var showThreadDump: Boolean = _

  @Option(names = Array("--show-loggers"), description = Array("Show logger levels"))
  private[master] var showLoggers: Boolean = _

  @Option(names = Array("--set-loglevel"), description = Array("Set logger level"))
  private[master] var setLogLevel: Boolean = _

  @Option(names = Array("--show-container-info"), description = Array("Show container info"))
  private[master] var showContainerInfo: Boolean = _

  @Option(
    names = Array("--add-cluster-alias"),
    paramLabel = "alias",
    description = Array("Add alias to use in the cli for the given set of masters"))
  private[master] var addClusterAlias: String = _

  @Option(
    names = Array("--remove-cluster-alias"),
    paramLabel = "alias",
    description = Array("Remove alias to use in the cli for the given set of masters"))
  private[master] var removeClusterAlias: String = _

  @Option(
    names = Array("--remove-workers-unavailable-info"),
    description = Array("Remove the workers unavailable info from the master."))
  private[master] var removeWorkersUnavailableInfo: Boolean = _

  @Option(
    names = Array("--revise-lost-shuffles"),
    description = Array("Revise lost shuffles or remove shuffles for an application."))
  private[master] var reviseLostShuffles: Boolean = _

  @Option(
    names = Array("--delete-apps"),
    description = Array("Delete resource of an application."))
  private[master] var deleteApps: Boolean = _

  @Option(
    names = Array("--update-interruption-notices"),
    paramLabel = "workerId1=timestamp,workerId2=timestamp,workerId3=timestamp",
    description = Array("Update interruption notices of workers."))
  private[master] var updateInterruptionNotices: String = _

  @Option(
    names = Array("--ratis-election-transfer"),
    description = Array("Transfer the ratis group leader to the peer specified by --peer-address."))
  private[master] var ratisElectionTransfer: Boolean = _

  @Option(
    names = Array("--ratis-election-step-down"),
    description = Array("Make the ratis group leader step down its leadership."))
  private[master] var ratisElectionStepDown: Boolean = _

  @Option(
    names = Array("--ratis-election-pause"),
    description = Array("Pause leader election at the current master." +
      " Then, the current master would not start a leader election."))
  private[master] var ratisElectionPause: Boolean = _

  @Option(
    names = Array("--ratis-election-resume"),
    description = Array("Resume leader election at the current master."))
  private[master] var ratisElectionResume: Boolean = _

  @Option(
    names = Array("--ratis-peer-add"),
    description = Array("Add new peers specified by --ratis-peers to the ratis group."))
  private[master] var ratisPeerAdd: Boolean = _

  @Option(
    names = Array("--ratis-peer-remove"),
    description = Array("Remove peers specified by --ratis-peers from the ratis group."))
  private[master] var ratisPeerRemove: Boolean = _

  @Option(
    names = Array("--ratis-peer-set-priority"),
    description = Array("Set the priority of the ratis peers specified by --peer-priorities."))
  private[master] var ratisPeerSetPriority: Boolean = _

  @Option(
    names = Array("--ratis-snapshot-create"),
    description = Array("Trigger the current master to take a ratis snapshot."))
  private[master] var ratisSnapshotCreate: Boolean = _

  @Option(
    names = Array("--ratis-download-raft-meta-conf"),
    paramLabel = "path",
    description = Array("Download the raft-meta.conf file of the current master" +
      " to the specified local file path."))
  private[master] var ratisDownloadRaftMetaConf: String = _

  @Option(
    names = Array("--ratis-generate-new-raft-meta-conf"),
    paramLabel = "path",
    description = Array("Generate a new-raft-meta.conf file based on the original raft-meta.conf" +
      " and the new peers specified by --ratis-peers, which is used to move a ratis node" +
      " to a new node. The generated file is saved to the specified local file path."))
  private[master] var ratisGenerateNewRaftMetaConf: String = _
}
