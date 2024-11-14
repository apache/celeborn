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

  @Option(names = Array("--show-cluster-apps"), description = Array("Show cluster applications"))
  private[master] var showClusterApps: Boolean = _

  @Option(names = Array("--show-cluster-shuffles"), description = Array("Show cluster shuffles"))
  private[master] var showClusterShuffles: Boolean = _

  @Option(
    names = Array("--show-top-disk-used-apps"),
    description = Array("Show top disk used apps"))
  private[master] var showTopDiskUsedApps: Boolean = _

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

  @Option(names = Array("--show-conf"), description = Array("Show master conf"))
  private[master] var showConf: Boolean = _

  @Option(names = Array("--show-dynamic-conf"), description = Array("Show dynamic master conf"))
  private[master] var showDynamicConf: Boolean = _

  @Option(names = Array("--show-thread-dump"), description = Array("Show master thread dump"))
  private[master] var showThreadDump: Boolean = _

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
}
