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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class WorkerData {
  private String host;
  private int rpcPort;
  private int pushPort;
  private int fetchPort;
  private int replicatePort;
  private int internalPort;
  private long slotsUsed;
  private Long lastHeartbeat;
  private Long heartbeatElapsedSeconds;
  private Map<String, String> diskInfos;
  private Map<String, String> resourceConsumption;
  private String workerRef;
  private String workerState;
  private Long workerStateStartTime;
  private Boolean isRegistered;
  private Boolean isShutdown;
  private Boolean isDecommissioning;

  public WorkerData(
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      int internalPort,
      long slotsUsed,
      Long lastHeartbeat,
      Long heartbeatElapsedSeconds,
      Map<String, String> diskInfos,
      Map<String, String> resourceConsumption,
      String workerRef,
      String workerState,
      Long workerStateStartTime) {
    this.host = host;
    this.rpcPort = rpcPort;
    this.pushPort = pushPort;
    this.fetchPort = fetchPort;
    this.replicatePort = replicatePort;
    this.internalPort = internalPort;
    this.slotsUsed = slotsUsed;
    this.lastHeartbeat = lastHeartbeat;
    this.heartbeatElapsedSeconds = heartbeatElapsedSeconds;
    this.diskInfos = diskInfos;
    this.resourceConsumption = resourceConsumption;
    this.workerRef = workerRef;
    this.workerState = workerState;
    this.workerStateStartTime = workerStateStartTime;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public int getPushPort() {
    return pushPort;
  }

  public void setPushPort(int pushPort) {
    this.pushPort = pushPort;
  }

  public int getFetchPort() {
    return fetchPort;
  }

  public void setFetchPort(int fetchPort) {
    this.fetchPort = fetchPort;
  }

  public int getReplicatePort() {
    return replicatePort;
  }

  public void setReplicatePort(int replicatePort) {
    this.replicatePort = replicatePort;
  }

  public int getInternalPort() {
    return internalPort;
  }

  public void setInternalPort(int internalPort) {
    this.internalPort = internalPort;
  }

  public long getSlotsUsed() {
    return slotsUsed;
  }

  public void setSlotsUsed(long slotsUsed) {
    this.slotsUsed = slotsUsed;
  }

  public Long getLastHeartbeat() {
    return lastHeartbeat;
  }

  public void setLastHeartbeat(Long lastHeartbeat) {
    this.lastHeartbeat = lastHeartbeat;
  }

  public Long getHeartbeatElapsedSeconds() {
    return heartbeatElapsedSeconds;
  }

  public void setHeartbeatElapsedSeconds(Long heartbeatElapsedSeconds) {
    this.heartbeatElapsedSeconds = heartbeatElapsedSeconds;
  }

  public Map<String, String> getDiskInfos() {
    if (diskInfos == null) {
      return Collections.EMPTY_MAP;
    }
    return diskInfos;
  }

  public void setDiskInfos(Map<String, String> diskInfos) {
    this.diskInfos = diskInfos;
  }

  public Map<String, String> getResourceConsumption() {
    if (resourceConsumption == null) {
      return Collections.EMPTY_MAP;
    }
    return resourceConsumption;
  }

  public void setResourceConsumption(Map<String, String> resourceConsumption) {
    this.resourceConsumption = resourceConsumption;
  }

  public String getWorkerRef() {
    return workerRef;
  }

  public void setWorkerRef(String workerRef) {
    this.workerRef = workerRef;
  }

  public String getWorkerState() {
    return workerState;
  }

  public void setWorkerState(String workerState) {
    this.workerState = workerState;
  }

  public Long getWorkerStateStartTime() {
    return workerStateStartTime;
  }

  public void setWorkerStateStartTime(Long workerStateStartTime) {
    this.workerStateStartTime = workerStateStartTime;
  }

  public Boolean getRegistered() {
    return isRegistered;
  }

  public void setRegistered(Boolean registered) {
    isRegistered = registered;
  }

  public Boolean getShutdown() {
    return isShutdown;
  }

  public void setShutdown(Boolean shutdown) {
    isShutdown = shutdown;
  }

  public Boolean getDecommissioning() {
    return isDecommissioning;
  }

  public void setDecommissioning(Boolean decommissioning) {
    isDecommissioning = decommissioning;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WorkerData that = (WorkerData) o;
    return getRpcPort() == that.getRpcPort()
        && getPushPort() == that.getPushPort()
        && getFetchPort() == that.getFetchPort()
        && getReplicatePort() == that.getReplicatePort()
        && getInternalPort() == that.getInternalPort()
        && getSlotsUsed() == that.getSlotsUsed()
        && Objects.equals(getHost(), that.getHost())
        && Objects.equals(getLastHeartbeat(), that.getLastHeartbeat())
        && Objects.equals(getHeartbeatElapsedSeconds(), that.getHeartbeatElapsedSeconds())
        && Objects.equals(getDiskInfos(), that.getDiskInfos())
        && Objects.equals(getResourceConsumption(), that.getResourceConsumption())
        && Objects.equals(getWorkerRef(), that.getWorkerRef())
        && Objects.equals(getWorkerState(), that.getWorkerState())
        && Objects.equals(getWorkerStateStartTime(), that.getWorkerStateStartTime())
        && Objects.equals(isRegistered, that.isRegistered)
        && Objects.equals(isShutdown, that.isShutdown)
        && Objects.equals(isDecommissioning, that.isDecommissioning);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getHost(),
        getRpcPort(),
        getPushPort(),
        getFetchPort(),
        getReplicatePort(),
        getInternalPort(),
        getSlotsUsed(),
        getLastHeartbeat(),
        getHeartbeatElapsedSeconds(),
        getDiskInfos(),
        getResourceConsumption(),
        getWorkerRef(),
        getWorkerState(),
        getWorkerStateStartTime(),
        isRegistered,
        isShutdown,
        isDecommissioning);
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
