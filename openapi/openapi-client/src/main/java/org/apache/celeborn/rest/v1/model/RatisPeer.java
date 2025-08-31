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
 * RatisPeer
 */
@JsonPropertyOrder({
  RatisPeer.JSON_PROPERTY_ID,
  RatisPeer.JSON_PROPERTY_ADDRESS,
  RatisPeer.JSON_PROPERTY_CLIENT_ADDRESS,
  RatisPeer.JSON_PROPERTY_ADMIN_ADDRESS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class RatisPeer {
  public static final String JSON_PROPERTY_ID = "id";
  private String id;

  public static final String JSON_PROPERTY_ADDRESS = "address";
  private String address;

  public static final String JSON_PROPERTY_CLIENT_ADDRESS = "clientAddress";
  private String clientAddress;

  public static final String JSON_PROPERTY_ADMIN_ADDRESS = "adminAddress";
  private String adminAddress;

  public RatisPeer() {
  }

  public RatisPeer id(String id) {
    
    this.id = id;
    return this;
  }

  /**
   * The id of the peer.
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

  public RatisPeer address(String address) {
    
    this.address = address;
    return this;
  }

  /**
   * The address of the peer.
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

  public RatisPeer clientAddress(String clientAddress) {
    
    this.clientAddress = clientAddress;
    return this;
  }

  /**
   * The address of the peer for client RPC communication.
   * @return clientAddress
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CLIENT_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getClientAddress() {
    return clientAddress;
  }


  @JsonProperty(JSON_PROPERTY_CLIENT_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setClientAddress(String clientAddress) {
    this.clientAddress = clientAddress;
  }

  public RatisPeer adminAddress(String adminAddress) {
    
    this.adminAddress = adminAddress;
    return this;
  }

  /**
   * The address of the peer for internal RPC communication.
   * @return adminAddress
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_ADMIN_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getAdminAddress() {
    return adminAddress;
  }


  @JsonProperty(JSON_PROPERTY_ADMIN_ADDRESS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setAdminAddress(String adminAddress) {
    this.adminAddress = adminAddress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RatisPeer ratisPeer = (RatisPeer) o;
    return Objects.equals(this.id, ratisPeer.id) &&
        Objects.equals(this.address, ratisPeer.address) &&
        Objects.equals(this.clientAddress, ratisPeer.clientAddress) &&
        Objects.equals(this.adminAddress, ratisPeer.adminAddress);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, address, clientAddress, adminAddress);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class RatisPeer {\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    address: ").append(toIndentedString(address)).append("\n");
    sb.append("    clientAddress: ").append(toIndentedString(clientAddress)).append("\n");
    sb.append("    adminAddress: ").append(toIndentedString(adminAddress)).append("\n");
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

