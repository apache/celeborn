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
 * RatisElectionTransferRequest
 */
@JsonPropertyOrder({
  RatisElectionTransferRequest.JSON_PROPERTY_PEER_ADDRESS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class RatisElectionTransferRequest {
  public static final String JSON_PROPERTY_PEER_ADDRESS = "peerAddress";
  @javax.annotation.Nonnull
  private String peerAddress;

  public RatisElectionTransferRequest() {
  }

  public RatisElectionTransferRequest peerAddress(@javax.annotation.Nonnull String peerAddress) {
    
    this.peerAddress = peerAddress;
    return this;
  }

  /**
   * The address of the peer to transfer the leader.
   * @return peerAddress
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_PEER_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getPeerAddress() {
    return peerAddress;
  }


  @JsonProperty(JSON_PROPERTY_PEER_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setPeerAddress(@javax.annotation.Nonnull String peerAddress) {
    this.peerAddress = peerAddress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RatisElectionTransferRequest ratisElectionTransferRequest = (RatisElectionTransferRequest) o;
    return Objects.equals(this.peerAddress, ratisElectionTransferRequest.peerAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(peerAddress);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class RatisElectionTransferRequest {\n");
    sb.append("    peerAddress: ").append(toIndentedString(peerAddress)).append("\n");
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

