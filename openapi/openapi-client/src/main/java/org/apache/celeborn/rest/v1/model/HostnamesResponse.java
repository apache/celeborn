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
 * HostnamesResponse
 */
@JsonPropertyOrder({
  HostnamesResponse.JSON_PROPERTY_HOSTNAMES
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class HostnamesResponse {
  public static final String JSON_PROPERTY_HOSTNAMES = "hostnames";
  @javax.annotation.Nonnull
  private List<String> hostnames = new ArrayList<>();

  public HostnamesResponse() {
  }

  public HostnamesResponse hostnames(@javax.annotation.Nonnull List<String> hostnames) {
    
    this.hostnames = hostnames;
    return this;
  }

  public HostnamesResponse addHostnamesItem(String hostnamesItem) {
    if (this.hostnames == null) {
      this.hostnames = new ArrayList<>();
    }
    this.hostnames.add(hostnamesItem);
    return this;
  }

  /**
   * The hostnames of the applications.
   * @return hostnames
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_HOSTNAMES)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public List<String> getHostnames() {
    return hostnames;
  }


  @JsonProperty(JSON_PROPERTY_HOSTNAMES)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setHostnames(@javax.annotation.Nonnull List<String> hostnames) {
    this.hostnames = hostnames;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HostnamesResponse hostnamesResponse = (HostnamesResponse) o;
    return Objects.equals(this.hostnames, hostnamesResponse.hostnames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostnames);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class HostnamesResponse {\n");
    sb.append("    hostnames: ").append(toIndentedString(hostnames)).append("\n");
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

