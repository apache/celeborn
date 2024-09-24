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


package org.apache.celeborn.rest.v1.model;

import java.util.Objects;
import java.util.Arrays;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.celeborn.rest.v1.model.MasterCommitData;
import org.apache.celeborn.rest.v1.model.MasterLeader;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * MasterInfoResponse
 */
@JsonPropertyOrder({
  MasterInfoResponse.JSON_PROPERTY_GROUP_ID,
  MasterInfoResponse.JSON_PROPERTY_LEADER,
  MasterInfoResponse.JSON_PROPERTY_MASTER_COMMIT_INFO
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class MasterInfoResponse {
  public static final String JSON_PROPERTY_GROUP_ID = "groupId";
  private String groupId;

  public static final String JSON_PROPERTY_LEADER = "leader";
  private MasterLeader leader;

  public static final String JSON_PROPERTY_MASTER_COMMIT_INFO = "masterCommitInfo";
  private List<MasterCommitData> masterCommitInfo = new ArrayList<>();

  public MasterInfoResponse() {
  }

  public MasterInfoResponse groupId(String groupId) {
    
    this.groupId = groupId;
    return this;
  }

  /**
   * The group id of the master raft server.
   * @return groupId
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_GROUP_ID)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getGroupId() {
    return groupId;
  }


  @JsonProperty(JSON_PROPERTY_GROUP_ID)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public MasterInfoResponse leader(MasterLeader leader) {
    
    this.leader = leader;
    return this;
  }

  /**
   * Get leader
   * @return leader
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_LEADER)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public MasterLeader getLeader() {
    return leader;
  }


  @JsonProperty(JSON_PROPERTY_LEADER)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setLeader(MasterLeader leader) {
    this.leader = leader;
  }

  public MasterInfoResponse masterCommitInfo(List<MasterCommitData> masterCommitInfo) {
    
    this.masterCommitInfo = masterCommitInfo;
    return this;
  }

  public MasterInfoResponse addMasterCommitInfoItem(MasterCommitData masterCommitInfoItem) {
    if (this.masterCommitInfo == null) {
      this.masterCommitInfo = new ArrayList<>();
    }
    this.masterCommitInfo.add(masterCommitInfoItem);
    return this;
  }

  /**
   * The raft commit info of the master.
   * @return masterCommitInfo
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_MASTER_COMMIT_INFO)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<MasterCommitData> getMasterCommitInfo() {
    return masterCommitInfo;
  }


  @JsonProperty(JSON_PROPERTY_MASTER_COMMIT_INFO)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setMasterCommitInfo(List<MasterCommitData> masterCommitInfo) {
    this.masterCommitInfo = masterCommitInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MasterInfoResponse masterInfoResponse = (MasterInfoResponse) o;
    return Objects.equals(this.groupId, masterInfoResponse.groupId) &&
        Objects.equals(this.leader, masterInfoResponse.leader) &&
        Objects.equals(this.masterCommitInfo, masterInfoResponse.masterCommitInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, leader, masterCommitInfo);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class MasterInfoResponse {\n");
    sb.append("    groupId: ").append(toIndentedString(groupId)).append("\n");
    sb.append("    leader: ").append(toIndentedString(leader)).append("\n");
    sb.append("    masterCommitInfo: ").append(toIndentedString(masterCommitInfo)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}

