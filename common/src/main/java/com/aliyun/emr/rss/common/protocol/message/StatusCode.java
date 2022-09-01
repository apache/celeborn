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

package com.aliyun.emr.rss.common.protocol.message;

public enum StatusCode {
  // 1/0 Status
  Success(0),
  PartialSuccess(1),
  Failed(2),

  // Specific Status
  ShuffleAlreadyRegistered(3),
  ShuffleNotRegistered(4),
  ReserveSlotFailed(5),
  SlotNotAvailable(6),
  WorkerNotFound(7),
  PartitionNotFound(8),
  SlavePartitionNotFound(9),
  DeleteFilesFailed(10),
  PartitionExists(11),
  ReviveFailed(12),
  PushDataFailed(13),
  NumMapperZero(14),
  MapEnded(15),
  StageEnded(16),

  // push data fail causes
  PushDataFailNonCriticalCause(17),
  PushDataFailSlave(18),
  PushDataFailMain(19),
  PushDataFailPartitionNotFound(20),

  HardSplit(21),
  SoftSplit(22),

  StageEndTimeOut(23),
  ShuffleDataLost(24);

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
    if (value == PushDataFailMain.getValue()) {
      msg = "PushDataFailMain";
    } else if (value == PushDataFailSlave.getValue()) {
      msg = "PushDataFailSlave";
    } else if (value == PushDataFailNonCriticalCause.getValue()) {
      msg = "PushDataFailNonCriticalCause";
    } else if (value == PushDataFailPartitionNotFound.getValue()) {
      msg = "PushDataFailPartitionNotFound";
    } else if (value == HardSplit.getValue()) {
      msg = "PartitionFileSplit";
    }
    return msg;
  }

  @Override
  public String toString() {
    return "StatusCode{" + "value=" + this.name() + '}';
  }
}
