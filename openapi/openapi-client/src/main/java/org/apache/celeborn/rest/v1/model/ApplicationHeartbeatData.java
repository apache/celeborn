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
 * ApplicationHeartbeatData
 */
@JsonPropertyOrder({
  ApplicationHeartbeatData.JSON_PROPERTY_APP_ID,
  ApplicationHeartbeatData.JSON_PROPERTY_LAST_HEARTBEAT_TIMESTAMP
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.7.0")
public class ApplicationHeartbeatData {
  public static final String JSON_PROPERTY_APP_ID = "appId";
  private String appId;

  public static final String JSON_PROPERTY_LAST_HEARTBEAT_TIMESTAMP = "lastHeartbeatTimestamp";
  private Long lastHeartbeatTimestamp;

  public ApplicationHeartbeatData() {
  }

  public ApplicationHeartbeatData appId(String appId) {
    
    this.appId = appId;
    return this;
  }

  /**
   * The id of the application.
   * @return appId
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_APP_ID)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getAppId() {
    return appId;
  }


  @JsonProperty(JSON_PROPERTY_APP_ID)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setAppId(String appId) {
    this.appId = appId;
  }

  public ApplicationHeartbeatData lastHeartbeatTimestamp(Long lastHeartbeatTimestamp) {
    
    this.lastHeartbeatTimestamp = lastHeartbeatTimestamp;
    return this;
  }

  /**
   * The last heartbeat timestamp of the application.
   * @return lastHeartbeatTimestamp
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_LAST_HEARTBEAT_TIMESTAMP)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Long getLastHeartbeatTimestamp() {
    return lastHeartbeatTimestamp;
  }


  @JsonProperty(JSON_PROPERTY_LAST_HEARTBEAT_TIMESTAMP)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setLastHeartbeatTimestamp(Long lastHeartbeatTimestamp) {
    this.lastHeartbeatTimestamp = lastHeartbeatTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ApplicationHeartbeatData applicationHeartbeatData = (ApplicationHeartbeatData) o;
    return Objects.equals(this.appId, applicationHeartbeatData.appId) &&
        Objects.equals(this.lastHeartbeatTimestamp, applicationHeartbeatData.lastHeartbeatTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(appId, lastHeartbeatTimestamp);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ApplicationHeartbeatData {\n");
    sb.append("    appId: ").append(toIndentedString(appId)).append("\n");
    sb.append("    lastHeartbeatTimestamp: ").append(toIndentedString(lastHeartbeatTimestamp)).append("\n");
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

