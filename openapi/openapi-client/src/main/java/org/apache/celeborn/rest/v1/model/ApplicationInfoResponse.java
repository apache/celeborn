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
import org.apache.celeborn.rest.v1.model.ApplicationInfo;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * ApplicationInfoResponse
 */
@JsonPropertyOrder({
  ApplicationInfoResponse.JSON_PROPERTY_APPLICATIONS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class ApplicationInfoResponse {
  public static final String JSON_PROPERTY_APPLICATIONS = "applications";
  private List<ApplicationInfo> applications = new ArrayList<>();

  public ApplicationInfoResponse() {
  }

  public ApplicationInfoResponse applications(List<ApplicationInfo> applications) {
    
    this.applications = applications;
    return this;
  }

  public ApplicationInfoResponse addApplicationsItem(ApplicationInfo applicationsItem) {
    if (this.applications == null) {
      this.applications = new ArrayList<>();
    }
    this.applications.add(applicationsItem);
    return this;
  }

  /**
   * The application information.
   * @return applications
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_APPLICATIONS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public List<ApplicationInfo> getApplications() {
    return applications;
  }


  @JsonProperty(JSON_PROPERTY_APPLICATIONS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setApplications(List<ApplicationInfo> applications) {
    this.applications = applications;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ApplicationInfoResponse applicationInfoResponse = (ApplicationInfoResponse) o;
    return Objects.equals(this.applications, applicationInfoResponse.applications);
  }

  @Override
  public int hashCode() {
    return Objects.hash(applications);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ApplicationInfoResponse {\n");
    sb.append("    applications: ").append(toIndentedString(applications)).append("\n");
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

