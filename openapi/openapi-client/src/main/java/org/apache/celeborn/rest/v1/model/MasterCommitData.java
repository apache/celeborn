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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * MasterCommitData
 */
@JsonPropertyOrder({
  MasterCommitData.JSON_PROPERTY_COMMIT_INDEX,
  MasterCommitData.JSON_PROPERTY_ID,
  MasterCommitData.JSON_PROPERTY_ADDRESS,
  MasterCommitData.JSON_PROPERTY_CLIENT_ADDRESS,
  MasterCommitData.JSON_PROPERTY_START_UP_ROLE,
  MasterCommitData.JSON_PROPERTY_PRIORITY
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class MasterCommitData {
  public static final String JSON_PROPERTY_COMMIT_INDEX = "commitIndex";
  private Long commitIndex;

  public static final String JSON_PROPERTY_ID = "id";
  private String id;

  public static final String JSON_PROPERTY_ADDRESS = "address";
  private String address;

  public static final String JSON_PROPERTY_CLIENT_ADDRESS = "clientAddress";
  private String clientAddress;

  public static final String JSON_PROPERTY_START_UP_ROLE = "startUpRole";
  private String startUpRole;

  public static final String JSON_PROPERTY_PRIORITY = "priority";
  private Integer priority;

  public MasterCommitData() {
  }

  public MasterCommitData commitIndex(Long commitIndex) {
    
    this.commitIndex = commitIndex;
    return this;
  }

  /**
   * The raft commit index of the master.
   * @return commitIndex
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_COMMIT_INDEX)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Long getCommitIndex() {
    return commitIndex;
  }


  @JsonProperty(JSON_PROPERTY_COMMIT_INDEX)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setCommitIndex(Long commitIndex) {
    this.commitIndex = commitIndex;
  }

  public MasterCommitData id(String id) {
    
    this.id = id;
    return this;
  }

  /**
   * The id of the raft peer.
   * @return id
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_ID)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getId() {
    return id;
  }


  @JsonProperty(JSON_PROPERTY_ID)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setId(String id) {
    this.id = id;
  }

  public MasterCommitData address(String address) {
    
    this.address = address;
    return this;
  }

  /**
   * The raft RPC address of the peer for server-server communication.
   * @return address
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getAddress() {
    return address;
  }


  @JsonProperty(JSON_PROPERTY_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setAddress(String address) {
    this.address = address;
  }

  public MasterCommitData clientAddress(String clientAddress) {
    
    this.clientAddress = clientAddress;
    return this;
  }

  /**
   * The raft RPC address of the peer for client operations.
   * @return clientAddress
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_CLIENT_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getClientAddress() {
    return clientAddress;
  }


  @JsonProperty(JSON_PROPERTY_CLIENT_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setClientAddress(String clientAddress) {
    this.clientAddress = clientAddress;
  }

  public MasterCommitData startUpRole(String startUpRole) {
    
    this.startUpRole = startUpRole;
    return this;
  }

  /**
   * The raft start up role of the master.
   * @return startUpRole
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_START_UP_ROLE)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getStartUpRole() {
    return startUpRole;
  }


  @JsonProperty(JSON_PROPERTY_START_UP_ROLE)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setStartUpRole(String startUpRole) {
    this.startUpRole = startUpRole;
  }

  public MasterCommitData priority(Integer priority) {
    
    this.priority = priority;
    return this;
  }

  /**
   * The raft peer priority.
   * @return priority
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_PRIORITY)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Integer getPriority() {
    return priority;
  }


  @JsonProperty(JSON_PROPERTY_PRIORITY)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setPriority(Integer priority) {
    this.priority = priority;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MasterCommitData masterCommitData = (MasterCommitData) o;
    return Objects.equals(this.commitIndex, masterCommitData.commitIndex) &&
        Objects.equals(this.id, masterCommitData.id) &&
        Objects.equals(this.address, masterCommitData.address) &&
        Objects.equals(this.clientAddress, masterCommitData.clientAddress) &&
        Objects.equals(this.startUpRole, masterCommitData.startUpRole) &&
        Objects.equals(this.priority, masterCommitData.priority);
  }

  @Override
  public int hashCode() {
    return Objects.hash(commitIndex, id, address, clientAddress, startUpRole, priority);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class MasterCommitData {\n");
    sb.append("    commitIndex: ").append(toIndentedString(commitIndex)).append("\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    address: ").append(toIndentedString(address)).append("\n");
    sb.append("    clientAddress: ").append(toIndentedString(clientAddress)).append("\n");
    sb.append("    startUpRole: ").append(toIndentedString(startUpRole)).append("\n");
    sb.append("    priority: ").append(toIndentedString(priority)).append("\n");
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

