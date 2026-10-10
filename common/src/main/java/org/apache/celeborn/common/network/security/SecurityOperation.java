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

package org.apache.celeborn.common.network.security;

/** Operation names supplied by Celeborn to connection security policies. */
public final class SecurityOperation {
  private SecurityOperation() {}

  // Native application compatibility and registration.
  public static final String APPLICATION_ACCESS = "APPLICATION_ACCESS";
  public static final String REGISTER_APPLICATION = "REGISTER_APPLICATION";

  // Master application requests.
  public static final String REGISTER_APPLICATION_INFO = "REGISTER_APPLICATION_INFO";
  public static final String APPLICATION_HEARTBEAT = "APPLICATION_HEARTBEAT";
  public static final String REQUEST_SLOTS = "REQUEST_SLOTS";
  public static final String REQUEST_WORKERS = "REQUEST_WORKERS";
  public static final String BATCH_UNREGISTER_SHUFFLES = "BATCH_UNREGISTER_SHUFFLES";
  public static final String UNREGISTER_SHUFFLE = "UNREGISTER_SHUFFLE";
  public static final String APPLICATION_LOST = "APPLICATION_LOST";
  public static final String REVISE_LOST_SHUFFLES = "REVISE_LOST_SHUFFLES";
  public static final String CHECK_QUOTA = "CHECK_QUOTA";
  public static final String CHECK_WORKERS_AVAILABLE = "CHECK_WORKERS_AVAILABLE";

  // Master and Worker service requests.
  public static final String REGISTER_WORKER = "REGISTER_WORKER";
  public static final String WORKER_HEARTBEAT = "WORKER_HEARTBEAT";
  public static final String REPORT_WORKER_UNAVAILABLE = "REPORT_WORKER_UNAVAILABLE";
  public static final String REPORT_WORKER_DECOMMISSION = "REPORT_WORKER_DECOMMISSION";
  public static final String EXCLUDE_WORKERS = "EXCLUDE_WORKERS";
  public static final String WORKER_LOST = "WORKER_LOST";
  public static final String WORKER_EVENT = "WORKER_EVENT";
  public static final String GET_APPLICATION_META = "GET_APPLICATION_META";
  public static final String REMOVE_WORKERS_UNAVAILABLE_INFO = "REMOVE_WORKERS_UNAVAILABLE_INFO";
  public static final String INSTALL_APPLICATION_META = "INSTALL_APPLICATION_META";

  // Worker control requests.
  public static final String RESERVE_SLOTS = "RESERVE_SLOTS";
  public static final String COMMIT_FILES = "COMMIT_FILES";
  public static final String DESTROY_WORKER_SLOTS = "DESTROY_WORKER_SLOTS";

  // LifecycleManager application requests.
  public static final String REGISTER_SHUFFLE = "REGISTER_SHUFFLE";
  public static final String REGISTER_MAP_PARTITION_TASK = "REGISTER_MAP_PARTITION_TASK";
  public static final String REVIVE = "REVIVE";
  public static final String PARTITION_SPLIT = "PARTITION_SPLIT";
  public static final String MAPPER_END = "MAPPER_END";
  public static final String READ_REDUCER_PARTITION_END = "READ_REDUCER_PARTITION_END";
  public static final String GET_REDUCER_FILE_GROUP = "GET_REDUCER_FILE_GROUP";
  public static final String GET_STAGE_END = "GET_STAGE_END";
  public static final String GET_SHUFFLE_ID = "GET_SHUFFLE_ID";
  public static final String REPORT_SHUFFLE_FETCH_FAILURE = "REPORT_SHUFFLE_FETCH_FAILURE";
  public static final String REPORT_BARRIER_STAGE_ATTEMPT_FAILURE =
      "REPORT_BARRIER_STAGE_ATTEMPT_FAILURE";

  // Shuffle data requests.
  public static final String PUSH_DATA = "PUSH_DATA";
  public static final String PUSH_MERGED_DATA = "PUSH_MERGED_DATA";
  public static final String PUSH_DATA_HANDSHAKE = "PUSH_DATA_HANDSHAKE";
  public static final String REGION_START = "REGION_START";
  public static final String REGION_FINISH = "REGION_FINISH";
  public static final String SEGMENT_START = "SEGMENT_START";
  public static final String OPEN_STREAM = "OPEN_STREAM";
  public static final String OPEN_STREAM_LIST = "OPEN_STREAM_LIST";
  public static final String CHUNK_FETCH = "CHUNK_FETCH";
  public static final String READ_ADD_CREDIT = "READ_ADD_CREDIT";
  public static final String NOTIFY_REQUIRED_SEGMENT = "NOTIFY_REQUIRED_SEGMENT";
  public static final String BUFFER_STREAM_END = "BUFFER_STREAM_END";
}
