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

package org.apache.celeborn.common.protocol;

import java.util.Objects;

import org.apache.celeborn.common.protocol.message.StatusCode;

public class ReviveRequest {
  public int shuffleId;
  public int mapId;
  public int attemptId;
  public int partitionId;
  public PartitionLocation loc;
  public int clientMaxEpoch;
  public StatusCode cause;
  public boolean urgent;
  public volatile int reviveStatus;

  public ReviveRequest(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      PartitionLocation loc,
      StatusCode cause,
      int clientMaxEpoch,
      boolean urgent) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptId = attemptId;
    this.partitionId = partitionId;
    this.loc = loc;
    this.clientMaxEpoch = clientMaxEpoch;
    this.cause = cause;
    this.urgent = urgent;
    reviveStatus = StatusCode.REVIVE_INITIALIZED.getValue();
  }

  @Override
  public String toString() {
    return "ReviveRequest{"
        + "shuffleId="
        + shuffleId
        + ", mapId="
        + mapId
        + ", attemptId="
        + attemptId
        + ", partitionId="
        + partitionId
        + ", loc="
        + loc
        + ", clientMaxEpoch="
        + clientMaxEpoch
        + ", cause="
        + cause
        + ", urgent="
        + urgent
        + ", reviveStatus="
        + reviveStatus
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReviveRequest that = (ReviveRequest) o;
    return shuffleId == that.shuffleId
        && mapId == that.mapId
        && attemptId == that.attemptId
        && partitionId == that.partitionId
        && clientMaxEpoch == that.clientMaxEpoch
        && urgent == that.urgent
        && reviveStatus == that.reviveStatus
        && loc == that.loc
        && cause == that.cause;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        shuffleId, mapId, attemptId, partitionId, loc, clientMaxEpoch, cause, urgent, reviveStatus);
  }
}
