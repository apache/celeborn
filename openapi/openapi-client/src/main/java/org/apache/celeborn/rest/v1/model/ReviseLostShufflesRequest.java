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
 * ReviseLostShufflesRequest
 */
@JsonPropertyOrder({
  ReviseLostShufflesRequest.JSON_PROPERTY_APP_ID,
  ReviseLostShufflesRequest.JSON_PROPERTY_SHUFFLE_IDS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class ReviseLostShufflesRequest {
  public static final String JSON_PROPERTY_APP_ID = "appId";
  @javax.annotation.Nullable
  private String appId;

  public static final String JSON_PROPERTY_SHUFFLE_IDS = "shuffleIds";
  @javax.annotation.Nullable
  private List<Integer> shuffleIds = new ArrayList<>();

  public ReviseLostShufflesRequest() {
  }

  public ReviseLostShufflesRequest appId(@javax.annotation.Nullable String appId) {
    
    this.appId = appId;
    return this;
  }

  /**
   * The application id.
   * @return appId
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_APP_ID)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getAppId() {
    return appId;
  }


  @JsonProperty(JSON_PROPERTY_APP_ID)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setAppId(@javax.annotation.Nullable String appId) {
    this.appId = appId;
  }

  public ReviseLostShufflesRequest shuffleIds(@javax.annotation.Nullable List<Integer> shuffleIds) {
    
    this.shuffleIds = shuffleIds;
    return this;
  }

  public ReviseLostShufflesRequest addShuffleIdsItem(Integer shuffleIdsItem) {
    if (this.shuffleIds == null) {
      this.shuffleIds = new ArrayList<>();
    }
    this.shuffleIds.add(shuffleIdsItem);
    return this;
  }

  /**
   * The shuffle ids to be revised.
   * @return shuffleIds
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_SHUFFLE_IDS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<Integer> getShuffleIds() {
    return shuffleIds;
  }


  @JsonProperty(JSON_PROPERTY_SHUFFLE_IDS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setShuffleIds(@javax.annotation.Nullable List<Integer> shuffleIds) {
    this.shuffleIds = shuffleIds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReviseLostShufflesRequest reviseLostShufflesRequest = (ReviseLostShufflesRequest) o;
    return Objects.equals(this.appId, reviseLostShufflesRequest.appId) &&
        Objects.equals(this.shuffleIds, reviseLostShufflesRequest.shuffleIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(appId, shuffleIds);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ReviseLostShufflesRequest {\n");
    sb.append("    appId: ").append(toIndentedString(appId)).append("\n");
    sb.append("    shuffleIds: ").append(toIndentedString(shuffleIds)).append("\n");
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

