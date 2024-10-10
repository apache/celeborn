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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * ContainerInfo
 */
@JsonPropertyOrder({
  ContainerInfo.JSON_PROPERTY_CONTAINER_NAME,
  ContainerInfo.JSON_PROPERTY_CONTAINER_ADDRESS,
  ContainerInfo.JSON_PROPERTY_CONTAINER_HOST_NAME,
  ContainerInfo.JSON_PROPERTY_CONTAINER_DATA_CENTER,
  ContainerInfo.JSON_PROPERTY_CONTAINER_AVAILABILITY_ZONE,
  ContainerInfo.JSON_PROPERTY_CONTAINER_USER,
  ContainerInfo.JSON_PROPERTY_CONTAINER_CLUSTER,
  ContainerInfo.JSON_PROPERTY_CONTAINER_TAGS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.7.0")
public class ContainerInfo {
  public static final String JSON_PROPERTY_CONTAINER_NAME = "containerName";
  private String containerName;

  public static final String JSON_PROPERTY_CONTAINER_ADDRESS = "containerAddress";
  private String containerAddress;

  public static final String JSON_PROPERTY_CONTAINER_HOST_NAME = "containerHostName";
  private String containerHostName;

  public static final String JSON_PROPERTY_CONTAINER_DATA_CENTER = "containerDataCenter";
  private String containerDataCenter;

  public static final String JSON_PROPERTY_CONTAINER_AVAILABILITY_ZONE = "containerAvailabilityZone";
  private String containerAvailabilityZone;

  public static final String JSON_PROPERTY_CONTAINER_USER = "containerUser";
  private String containerUser;

  public static final String JSON_PROPERTY_CONTAINER_CLUSTER = "containerCluster";
  private String containerCluster;

  public static final String JSON_PROPERTY_CONTAINER_TAGS = "containerTags";
  private List<String> containerTags = new ArrayList<>();

  public ContainerInfo() {
  }

  public ContainerInfo containerName(String containerName) {
    
    this.containerName = containerName;
    return this;
  }

  /**
   * The name of the container.
   * @return containerName
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CONTAINER_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getContainerName() {
    return containerName;
  }


  @JsonProperty(JSON_PROPERTY_CONTAINER_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setContainerName(String containerName) {
    this.containerName = containerName;
  }

  public ContainerInfo containerAddress(String containerAddress) {
    
    this.containerAddress = containerAddress;
    return this;
  }

  /**
   * The IP address of the container.
   * @return containerAddress
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CONTAINER_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getContainerAddress() {
    return containerAddress;
  }


  @JsonProperty(JSON_PROPERTY_CONTAINER_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setContainerAddress(String containerAddress) {
    this.containerAddress = containerAddress;
  }

  public ContainerInfo containerHostName(String containerHostName) {
    
    this.containerHostName = containerHostName;
    return this;
  }

  /**
   * The hostname of the container.
   * @return containerHostName
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CONTAINER_HOST_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getContainerHostName() {
    return containerHostName;
  }


  @JsonProperty(JSON_PROPERTY_CONTAINER_HOST_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setContainerHostName(String containerHostName) {
    this.containerHostName = containerHostName;
  }

  public ContainerInfo containerDataCenter(String containerDataCenter) {
    
    this.containerDataCenter = containerDataCenter;
    return this;
  }

  /**
   * The datacenter of the container.
   * @return containerDataCenter
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CONTAINER_DATA_CENTER)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getContainerDataCenter() {
    return containerDataCenter;
  }


  @JsonProperty(JSON_PROPERTY_CONTAINER_DATA_CENTER)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setContainerDataCenter(String containerDataCenter) {
    this.containerDataCenter = containerDataCenter;
  }

  public ContainerInfo containerAvailabilityZone(String containerAvailabilityZone) {
    
    this.containerAvailabilityZone = containerAvailabilityZone;
    return this;
  }

  /**
   * The availability zone of the container.
   * @return containerAvailabilityZone
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CONTAINER_AVAILABILITY_ZONE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getContainerAvailabilityZone() {
    return containerAvailabilityZone;
  }


  @JsonProperty(JSON_PROPERTY_CONTAINER_AVAILABILITY_ZONE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setContainerAvailabilityZone(String containerAvailabilityZone) {
    this.containerAvailabilityZone = containerAvailabilityZone;
  }

  public ContainerInfo containerUser(String containerUser) {
    
    this.containerUser = containerUser;
    return this;
  }

  /**
   * The user of the container.
   * @return containerUser
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CONTAINER_USER)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getContainerUser() {
    return containerUser;
  }


  @JsonProperty(JSON_PROPERTY_CONTAINER_USER)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setContainerUser(String containerUser) {
    this.containerUser = containerUser;
  }

  public ContainerInfo containerCluster(String containerCluster) {
    
    this.containerCluster = containerCluster;
    return this;
  }

  /**
   * The cluster that the container is part of.
   * @return containerCluster
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CONTAINER_CLUSTER)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getContainerCluster() {
    return containerCluster;
  }


  @JsonProperty(JSON_PROPERTY_CONTAINER_CLUSTER)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setContainerCluster(String containerCluster) {
    this.containerCluster = containerCluster;
  }

  public ContainerInfo containerTags(List<String> containerTags) {
    
    this.containerTags = containerTags;
    return this;
  }

  public ContainerInfo addContainerTagsItem(String containerTagsItem) {
    if (this.containerTags == null) {
      this.containerTags = new ArrayList<>();
    }
    this.containerTags.add(containerTagsItem);
    return this;
  }

  /**
   * The tags applied to this container if any.
   * @return containerTags
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CONTAINER_TAGS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<String> getContainerTags() {
    return containerTags;
  }


  @JsonProperty(JSON_PROPERTY_CONTAINER_TAGS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setContainerTags(List<String> containerTags) {
    this.containerTags = containerTags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContainerInfo containerInfo = (ContainerInfo) o;
    return Objects.equals(this.containerName, containerInfo.containerName) &&
        Objects.equals(this.containerAddress, containerInfo.containerAddress) &&
        Objects.equals(this.containerHostName, containerInfo.containerHostName) &&
        Objects.equals(this.containerDataCenter, containerInfo.containerDataCenter) &&
        Objects.equals(this.containerAvailabilityZone, containerInfo.containerAvailabilityZone) &&
        Objects.equals(this.containerUser, containerInfo.containerUser) &&
        Objects.equals(this.containerCluster, containerInfo.containerCluster) &&
        Objects.equals(this.containerTags, containerInfo.containerTags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(containerName, containerAddress, containerHostName, containerDataCenter, containerAvailabilityZone, containerUser, containerCluster, containerTags);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ContainerInfo {\n");
    sb.append("    containerName: ").append(toIndentedString(containerName)).append("\n");
    sb.append("    containerAddress: ").append(toIndentedString(containerAddress)).append("\n");
    sb.append("    containerHostName: ").append(toIndentedString(containerHostName)).append("\n");
    sb.append("    containerDataCenter: ").append(toIndentedString(containerDataCenter)).append("\n");
    sb.append("    containerAvailabilityZone: ").append(toIndentedString(containerAvailabilityZone)).append("\n");
    sb.append("    containerUser: ").append(toIndentedString(containerUser)).append("\n");
    sb.append("    containerCluster: ").append(toIndentedString(containerCluster)).append("\n");
    sb.append("    containerTags: ").append(toIndentedString(containerTags)).append("\n");
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

