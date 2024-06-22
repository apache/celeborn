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
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class MasterGroupData {
  private String groupId;
  private MasterLeader leader;
  private List<MasterCommitData> masterCommitInfo;

  public MasterGroupData(
      String groupId, MasterLeader leader, List<MasterCommitData> masterCommitInfo) {
    this.groupId = groupId;
    this.leader = leader;
    this.masterCommitInfo = masterCommitInfo;
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public MasterLeader getLeader() {
    return leader;
  }

  public void setLeader(MasterLeader leader) {
    this.leader = leader;
  }

  public List<MasterCommitData> getMasterCommitInfo() {
    if (masterCommitInfo == null) {
      return Collections.EMPTY_LIST;
    }
    return masterCommitInfo;
  }

  public void setMasterCommitInfo(List<MasterCommitData> masterCommitInfo) {
    this.masterCommitInfo = masterCommitInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MasterGroupData that = (MasterGroupData) o;
    return Objects.equals(getGroupId(), that.getGroupId())
        && Objects.equals(getLeader(), that.getLeader())
        && Objects.equals(getMasterCommitInfo(), that.getMasterCommitInfo());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getGroupId(), getLeader(), getMasterCommitInfo());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
