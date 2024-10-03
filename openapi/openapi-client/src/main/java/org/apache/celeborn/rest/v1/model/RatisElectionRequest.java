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
import org.apache.celeborn.rest.v1.model.RatisPeer;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * RatisElectionRequest
 */
@JsonPropertyOrder({
  RatisElectionRequest.JSON_PROPERTY_PEER
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class RatisElectionRequest {
  public static final String JSON_PROPERTY_PEER = "peer";
  private RatisPeer peer;

  public RatisElectionRequest() {
  }

  public RatisElectionRequest peer(RatisPeer peer) {
    
    this.peer = peer;
    return this;
  }

  /**
   * Get peer
   * @return peer
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_PEER)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public RatisPeer getPeer() {
    return peer;
  }


  @JsonProperty(JSON_PROPERTY_PEER)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setPeer(RatisPeer peer) {
    this.peer = peer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RatisElectionRequest ratisElectionRequest = (RatisElectionRequest) o;
    return Objects.equals(this.peer, ratisElectionRequest.peer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(peer);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class RatisElectionRequest {\n");
    sb.append("    peer: ").append(toIndentedString(peer)).append("\n");
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

