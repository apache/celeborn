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

package org.apache.celeborn.cli.worker

import picocli.CommandLine.Option

final class WorkerOptions {

  @Option(names = Array("--show-worker-info"), description = Array("Show worker info"))
  private[worker] var showWorkerInfo: Boolean = _

  @Option(
    names = Array("--show-apps-on-worker"),
    description = Array("Show applications running on the worker"))
  private[worker] var showAppsOnWorker: Boolean = _

  @Option(
    names = Array("--show-shuffles-on-worker"),
    description = Array("Show shuffles running on the worker"))
  private[worker] var showShufflesOnWorker: Boolean = _

  @Option(
    names = Array("--show-top-disk-used-apps"),
    description = Array("Show top disk used applications"))
  private[worker] var showTopDiskUsedApps: Boolean = _

  @Option(
    names = Array("--show-partition-location-info"),
    description = Array("Show partition location information"))
  private[worker] var showPartitionLocationInfo: Boolean = _

  @Option(names = Array("--show-unavailable-peers"), description = Array("Show unavailable peers"))
  private[worker] var showUnavailablePeers: Boolean = _

  @Option(names = Array("--is-shutdown"), description = Array("Check if the system is shutdown"))
  private[worker] var isShutdown: Boolean = _

  @Option(
    names = Array("--is-decommissioning"),
    description = Array("Check if the system is decommissioning"))
  private[worker] var isDecommissioning: Boolean = _

  @Option(
    names = Array("--is-registered"),
    description = Array("Check if the system is registered"))
  private[worker] var isRegistered: Boolean = _

  @Option(
    names = Array("--exit"),
    paramLabel = "exit_type",
    description = Array("Exit the application with a specified type"))
  private[worker] var exitType: String = _

  @Option(names = Array("--show-conf"), description = Array("Show worker conf"))
  private[worker] var showConf: Boolean = _

  @Option(names = Array("--show-dynamic-conf"), description = Array("Show dynamic worker conf"))
  private[worker] var showDynamicConf: Boolean = _

  @Option(names = Array("--show-thread-dump"), description = Array("Show worker thread dump"))
  private[worker] var showThreadDump: Boolean = _

}
