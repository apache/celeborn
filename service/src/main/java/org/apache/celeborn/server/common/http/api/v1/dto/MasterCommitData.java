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

public class MasterCommitData {
  private Long commitIndex;
  private String id;
  private String address;
  private String clientAddress;
  private String startupRole;

  public MasterCommitData(
      Long commitIndex, String id, String address, String clientAddress, String startupRole) {
    this.commitIndex = commitIndex;
    this.id = id;
    this.address = address;
    this.clientAddress = clientAddress;
    this.startupRole = startupRole;
  }

  public Long getCommitIndex() {
    return commitIndex;
  }

  public void setCommitIndex(Long commitIndex) {
    this.commitIndex = commitIndex;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public String getClientAddress() {
    return clientAddress;
  }

  public void setClientAddress(String clientAddress) {
    this.clientAddress = clientAddress;
  }

  public String getStartupRole() {
    return startupRole;
  }

  public void setStartupRole(String startupRole) {
    this.startupRole = startupRole;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MasterCommitData that = (MasterCommitData) o;
    return Objects.equals(getCommitIndex(), that.getCommitIndex())
        && Objects.equals(getId(), that.getId())
        && Objects.equals(getAddress(), that.getAddress())
        && Objects.equals(getClientAddress(), that.getClientAddress())
        && Objects.equals(getStartupRole(), that.getStartupRole());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getCommitIndex(), getId(), getAddress(), getClientAddress(), getStartupRole());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
