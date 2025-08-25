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
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * ApplicationInfo
 */
@JsonPropertyOrder({
  ApplicationInfo.JSON_PROPERTY_APP_ID,
  ApplicationInfo.JSON_PROPERTY_USER_IDENTIFIER,
  ApplicationInfo.JSON_PROPERTY_EXTRA_INFO,
  ApplicationInfo.JSON_PROPERTY_REGISTRATION_TIME
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class ApplicationInfo {
  public static final String JSON_PROPERTY_APP_ID = "appId";
  private String appId;

  public static final String JSON_PROPERTY_USER_IDENTIFIER = "userIdentifier";
  private String userIdentifier;

  public static final String JSON_PROPERTY_EXTRA_INFO = "extraInfo";
  private Map<String, String> extraInfo = new HashMap<>();

  public static final String JSON_PROPERTY_REGISTRATION_TIME = "registrationTime";
  private Long registrationTime;

  public ApplicationInfo() {
  }

  public ApplicationInfo appId(String appId) {
    
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

  public ApplicationInfo userIdentifier(String userIdentifier) {
    
    this.userIdentifier = userIdentifier;
    return this;
  }

  /**
   * The user identifier of the application.
   * @return userIdentifier
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_USER_IDENTIFIER)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getUserIdentifier() {
    return userIdentifier;
  }


  @JsonProperty(JSON_PROPERTY_USER_IDENTIFIER)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setUserIdentifier(String userIdentifier) {
    this.userIdentifier = userIdentifier;
  }

  public ApplicationInfo extraInfo(Map<String, String> extraInfo) {
    
    this.extraInfo = extraInfo;
    return this;
  }

  public ApplicationInfo putExtraInfoItem(String key, String extraInfoItem) {
    if (this.extraInfo == null) {
      this.extraInfo = new HashMap<>();
    }
    this.extraInfo.put(key, extraInfoItem);
    return this;
  }

  /**
   * Extra information of the application.
   * @return extraInfo
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_EXTRA_INFO)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Map<String, String> getExtraInfo() {
    return extraInfo;
  }


  @JsonProperty(JSON_PROPERTY_EXTRA_INFO)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setExtraInfo(Map<String, String> extraInfo) {
    this.extraInfo = extraInfo;
  }

  public ApplicationInfo registrationTime(Long registrationTime) {
    
    this.registrationTime = registrationTime;
    return this;
  }

  /**
   * The registration time of the application.
   * @return registrationTime
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_REGISTRATION_TIME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getRegistrationTime() {
    return registrationTime;
  }


  @JsonProperty(JSON_PROPERTY_REGISTRATION_TIME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setRegistrationTime(Long registrationTime) {
    this.registrationTime = registrationTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ApplicationInfo applicationInfo = (ApplicationInfo) o;
    return Objects.equals(this.appId, applicationInfo.appId) &&
        Objects.equals(this.userIdentifier, applicationInfo.userIdentifier) &&
        Objects.equals(this.extraInfo, applicationInfo.extraInfo) &&
        Objects.equals(this.registrationTime, applicationInfo.registrationTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(appId, userIdentifier, extraInfo, registrationTime);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ApplicationInfo {\n");
    sb.append("    appId: ").append(toIndentedString(appId)).append("\n");
    sb.append("    userIdentifier: ").append(toIndentedString(userIdentifier)).append("\n");
    sb.append("    extraInfo: ").append(toIndentedString(extraInfo)).append("\n");
    sb.append("    registrationTime: ").append(toIndentedString(registrationTime)).append("\n");
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

