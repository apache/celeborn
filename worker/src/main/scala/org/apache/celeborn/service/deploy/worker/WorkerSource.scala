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

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.common.metrics.source.AbstractSource

class WorkerSource(conf: CelebornConf) extends AbstractSource(conf, MetricsSystem.ROLE_WORKER) {
  override val sourceName = "worker"

  import WorkerSource._
  // add counters
  addCounter(PUSH_DATA_FAIL_COUNT)

  // add Timers
  addTimer(COMMIT_FILES_TIME)
  addTimer(RESERVE_SLOTS_TIME)
  addTimer(FLUSH_DATA_TIME)
  addTimer(MASTER_PUSH_DATA_TIME)
  addTimer(SLAVE_PUSH_DATA_TIME)

  addTimer(MASTER_PUSH_DATA_HAND_SHAKE_TIME)
  addTimer(SLAVE_PUSH_DATA_HAND_SHAKE_TIME)
  addTimer(MASTER_REGION_START_TIME)
  addTimer(SLAVE_REGION_START_TIME)
  addTimer(MASTER_REGION_FINISH_TIME)
  addTimer(SLAVE_REGION_FINISH_TIME)

  addTimer(FETCH_CHUNK_TIME)
  addTimer(OPEN_STREAM_TIME)
  addTimer(TAKE_BUFFER_TIME)
  addTimer(PARTITION_SORT_TIME)

  // start cleaner thread
  startCleaner()
}

object WorkerSource {
  val SERVLET_PATH = "/metrics/prometheus"

  val COMMIT_FILES_TIME = "CommitFilesTime"

  val RESERVE_SLOTS_TIME = "ReserveSlotsTime"

  val FLUSH_DATA_TIME = "FlushDataTime"

  val OPEN_STREAM_TIME = "OpenStreamTime"

  val FETCH_CHUNK_TIME = "FetchChunkTime"

  // push data
  val MASTER_PUSH_DATA_TIME = "MasterPushDataTime"
  val SLAVE_PUSH_DATA_TIME = "SlavePushDataTime"
  val PUSH_DATA_FAIL_COUNT = "PushDataFailCount"
  val PUSH_DATA_HAND_SHAKE_FAIL_COUNT = "PushDataHandshakeFailCount"
  val REGION_START_FAIL_COUNT = "RegionStartFailCount"
  val REGION_FINISH_FAIL_COUNT = "RegionFinishFailCount"
  val MASTER_PUSH_DATA_HAND_SHAKE_TIME = "MasterPushDataHandshakeTime"
  val SLAVE_PUSH_DATA_HAND_SHAKE_TIME = "SlavePushDataHandshakeTime"
  val MASTER_REGION_START_TIME = "MasterRegionStartTime"
  val SLAVE_REGION_START_TIME = "SlaveRegionStartTime"
  val MASTER_REGION_FINISH_TIME = "MasterRegionFinishTime"
  val SLAVE_REGION_FINISH_TIME = "SlaveRegionFinishTime"

  // flush
  val TAKE_BUFFER_TIME = "TakeBufferTime"
  val TAKE_BUFFER_TIME_INDEX = "TakeBufferTimeIndex"

  val REGISTERED_SHUFFLE_COUNT = "RegisteredShuffleCount"

  // slots
  val SLOTS_ALLOCATED = "SlotsAllocated"

  // memory
  val NETTY_MEMORY = "NettyMemory"
  val PARTITION_SORT_TIME = "SortTime"
  val PARTITION_SORTER_MEMORY = "SortMemory"
  val PARTITION_SORTING_FILES = "SortingFiles"
  val PARTITION_SORTED_FILES = "SortedFiles"
  val PARTITION_SORTED_FILES_SIZE = "SortedFileSize"
  val DISK_BUFFER = "DiskBuffer"
  val PAUSE_PUSH_DATA_COUNT = "PausePushData"
  val PAUSE_PUSH_DATA_AND_REPLICATE_COUNT = "PausePushDataAndReplicate"
  val BUFFER_STREAM_READ_BUFFER = "BufferStreamReadBuffer"
  val READ_BUFFER_DISPATCHER_REQUEST_LENGTH = "ReadBufferDispatcherRequestsLength"

  // local device
  val DEVICE_OS_FREE_CAPACITY = "DeviceOSFreeCapacity(B)"
  val DEVICE_OS_TOTAL_CAPACITY = "DeviceOSTotalCapacity(B)"
  val DEVICE_CELEBORN_FREE_CAPACITY = "DeviceCelebornFreeCapacity(B)"
  val DEVICE_CELEBORN_TOTAL_CAPACITY = "DeviceCelebornTotalCapacity(B)"

  // Congestion control
  val POTENTIAL_CONSUME_SPEED = "PotentialConsumeSpeed"
  val USER_PRODUCE_SPEED = "UserProduceSpeed"
}
