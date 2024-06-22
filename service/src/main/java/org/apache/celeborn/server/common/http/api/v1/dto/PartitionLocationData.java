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

package org.apache.celeborn.server.common.http.api.v1.dto;

import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class PartitionLocationData {
  private String idEpoch;
  private String hostAndPorts;
  private String mode;
  private String peer;
  private String storage;
  private String mapIdBitMap;

  public PartitionLocationData(
      String idEpoch,
      String hostAndPorts,
      String mode,
      String peer,
      String storage,
      String mapIdBitMap) {
    this.idEpoch = idEpoch;
    this.hostAndPorts = hostAndPorts;
    this.mode = mode;
    this.peer = peer;
    this.storage = storage;
    this.mapIdBitMap = mapIdBitMap;
  }

  public String getIdEpoch() {
    return idEpoch;
  }

  public void setIdEpoch(String idEpoch) {
    this.idEpoch = idEpoch;
  }

  public String getHostAndPorts() {
    return hostAndPorts;
  }

  public void setHostAndPorts(String hostAndPorts) {
    this.hostAndPorts = hostAndPorts;
  }

  public String getMode() {
    return mode;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public String getPeer() {
    return peer;
  }

  public void setPeer(String peer) {
    this.peer = peer;
  }

  public String getStorage() {
    return storage;
  }

  public void setStorage(String storage) {
    this.storage = storage;
  }

  public String getMapIdBitMap() {
    return mapIdBitMap;
  }

  public void setMapIdBitMap(String mapIdBitMap) {
    this.mapIdBitMap = mapIdBitMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PartitionLocationData that = (PartitionLocationData) o;
    return Objects.equals(getIdEpoch(), that.getIdEpoch())
        && Objects.equals(getHostAndPorts(), that.getHostAndPorts())
        && Objects.equals(getMode(), that.getMode())
        && Objects.equals(getPeer(), that.getPeer())
        && Objects.equals(getStorage(), that.getStorage())
        && Objects.equals(getMapIdBitMap(), that.getMapIdBitMap());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getIdEpoch(), getHostAndPorts(), getMode(), getPeer(), getStorage(), getMapIdBitMap());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
