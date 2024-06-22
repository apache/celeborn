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

public class ApplicationHeartbeatData {
  private String appId;
  private Long lastHeartbeat;

  public ApplicationHeartbeatData(String appId, Long lastHeartbeat) {
    this.appId = appId;
    this.lastHeartbeat = lastHeartbeat;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public Long getLastHeartbeat() {
    return lastHeartbeat;
  }

  public void setLastHeartbeat(Long lastHeartbeat) {
    this.lastHeartbeat = lastHeartbeat;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ApplicationHeartbeatData that = (ApplicationHeartbeatData) o;
    return Objects.equals(getAppId(), that.getAppId())
        && Objects.equals(getLastHeartbeat(), that.getLastHeartbeat());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getAppId(), getLastHeartbeat());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
