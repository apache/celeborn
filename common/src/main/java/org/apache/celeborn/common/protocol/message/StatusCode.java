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

package org.apache.celeborn.common.protocol.message;

public enum StatusCode {
  // 1/0 Status
  SUCCESS(0),
  PARTIAL_SUCCESS(1),
  FAILED(2),

  // Specific Status
  SHUFFLE_ALREADY_REGISTERED(3),
  SHUFFLE_NOT_REGISTERED(4),
  RESERVE_SLOTS_FAILED(5),
  SLOT_NOT_AVAILABLE(6),
  WORKER_NOT_FOUND(7),
  PARTITION_NOT_FOUND(8),
  SLAVE_PARTITION_NOT_FOUND(9),
  DELETE_FILES_FAILED(10),
  PARTITION_EXISTS(11),
  REVIVE_FAILED(12),
  PUSH_DATA_FAILED(13),
  NUM_MAPPER_ZERO(14),
  MAP_ENDED(15),
  STAGE_ENDED(16),

  // push data fail causes
  PUSH_DATA_FAIL_NON_CRITICAL_CAUSE(17),
  PUSH_DATA_FAIL_SLAVE(18),
  PUSH_DATA_FAIL_MASTER(19),
  PUSH_DATA_FAIL_PARTITION_NOT_FOUND(20),

  HARD_SPLIT(21),
  SOFT_SPLIT(22),

  STAGE_END_TIME_OUT(23),
  SHUFFLE_DATA_LOST(24),
  WORKER_SHUTDOWN(25),
  NO_AVAILABLE_WORKING_DIR(26),
  WORKER_IN_BLACKLIST(27),
  UNKNOWN_WORKER(28),

  COMMIT_FILE_EXCEPTION(29);

  private final byte value;

  StatusCode(int value) {
    assert (value >= 0 && value < 256);
    this.value = (byte) value;
  }

  public final byte getValue() {
    return value;
  }

  public String getMessage() {
    String msg = "";
    if (value == PUSH_DATA_FAIL_MASTER.getValue()) {
      msg = "PushDataFailMaster";
    } else if (value == PUSH_DATA_FAIL_SLAVE.getValue()) {
      msg = "PushDataFailSlave";
    } else if (value == PUSH_DATA_FAIL_NON_CRITICAL_CAUSE.getValue()) {
      msg = "PushDataFailNonCriticalCause";
    } else if (value == PUSH_DATA_FAIL_PARTITION_NOT_FOUND.getValue()) {
      msg = "PushDataFailPartitionNotFound";
    } else if (value == HARD_SPLIT.getValue()) {
      msg = "PartitionFileSplit";
    }
    return msg;
  }

  @Override
  public String toString() {
    return "StatusCode{" + "value=" + this.name() + '}';
  }
}
