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

export interface WorkerDetail {
  diskInfos: WorkerDiskInfo[]
  dynamicConfigs: Record<string, string>
  fetchPort: number
  flameGraph: string
  hostname: string
  ip: string
  logFiles: Record<string, string>
  logs: string
  memoryInfo: WorkerMemoryInfo
  metrics: string
  pushPort: number
  replicatePort: number
  startTime: Date
  state: string
  staticConfigs: Record<string, string>
  threadDump: string
}

export interface WorkerDiskInfo {
  diskType: string
  mountPoint: string
  totalSpace: string
  usedPercent: string
  usedSpace: string
}

export interface WorkerMemoryInfo {
  diskBuffer: string
  pausePushDataThreshold: string
  pauseReplicateThreshold: string
  readBuffer: string
  resumeThreshold: string
  sortMemory: string
  totalNativeMemory: string
  usedDirectMemory: string
  [key: string]: string
}
