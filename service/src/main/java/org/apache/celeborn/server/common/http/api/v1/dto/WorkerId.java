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

import org.apache.celeborn.common.meta.WorkerInfo;

public class WorkerId {
  private String host;
  private int rpcPort;
  private int pushPort;
  private int fetchPort;
  private int replicatePort;

  public WorkerId(String host, int rpcPort, int pushPort, int fetchPort, int replicatePort) {
    this.host = host;
    this.rpcPort = rpcPort;
    this.pushPort = pushPort;
    this.fetchPort = fetchPort;
    this.replicatePort = replicatePort;
  }

  public WorkerInfo toWorkerInfo() {
    return new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WorkerId workerId = (WorkerId) o;
    return getRpcPort() == workerId.getRpcPort()
        && getPushPort() == workerId.getPushPort()
        && getFetchPort() == workerId.getFetchPort()
        && getReplicatePort() == workerId.getReplicatePort()
        && Objects.equals(getHost(), workerId.getHost());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getHost(), getRpcPort(), getPushPort(), getFetchPort(), getReplicatePort());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
