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
 * AppDiskUsageData
 */
@JsonPropertyOrder({
  AppDiskUsageData.JSON_PROPERTY_APP_ID,
  AppDiskUsageData.JSON_PROPERTY_ESTIMATED_USAGE,
  AppDiskUsageData.JSON_PROPERTY_ESTIMATED_USAGE_STR
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class AppDiskUsageData {
  public static final String JSON_PROPERTY_APP_ID = "appId";
  private String appId;

  public static final String JSON_PROPERTY_ESTIMATED_USAGE = "estimatedUsage";
  private Long estimatedUsage;

  public static final String JSON_PROPERTY_ESTIMATED_USAGE_STR = "estimatedUsageStr";
  private String estimatedUsageStr;

  public AppDiskUsageData() {
  }

  public AppDiskUsageData appId(String appId) {
    
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

  public AppDiskUsageData estimatedUsage(Long estimatedUsage) {
    
    this.estimatedUsage = estimatedUsage;
    return this;
  }

  /**
   * The application disk usage.
   * @return estimatedUsage
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_ESTIMATED_USAGE)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Long getEstimatedUsage() {
    return estimatedUsage;
  }


  @JsonProperty(JSON_PROPERTY_ESTIMATED_USAGE)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setEstimatedUsage(Long estimatedUsage) {
    this.estimatedUsage = estimatedUsage;
  }

  public AppDiskUsageData estimatedUsageStr(String estimatedUsageStr) {
    
    this.estimatedUsageStr = estimatedUsageStr;
    return this;
  }

  /**
   * The application disk usage in string type.
   * @return estimatedUsageStr
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_ESTIMATED_USAGE_STR)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getEstimatedUsageStr() {
    return estimatedUsageStr;
  }


  @JsonProperty(JSON_PROPERTY_ESTIMATED_USAGE_STR)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setEstimatedUsageStr(String estimatedUsageStr) {
    this.estimatedUsageStr = estimatedUsageStr;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AppDiskUsageData appDiskUsageData = (AppDiskUsageData) o;
    return Objects.equals(this.appId, appDiskUsageData.appId) &&
        Objects.equals(this.estimatedUsage, appDiskUsageData.estimatedUsage) &&
        Objects.equals(this.estimatedUsageStr, appDiskUsageData.estimatedUsageStr);
  }

  @Override
  public int hashCode() {
    return Objects.hash(appId, estimatedUsage, estimatedUsageStr);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AppDiskUsageData {\n");
    sb.append("    appId: ").append(toIndentedString(appId)).append("\n");
    sb.append("    estimatedUsage: ").append(toIndentedString(estimatedUsage)).append("\n");
    sb.append("    estimatedUsageStr: ").append(toIndentedString(estimatedUsageStr)).append("\n");
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

