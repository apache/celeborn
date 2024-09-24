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
 * WorkerId
 */
@JsonPropertyOrder({
  WorkerId.JSON_PROPERTY_HOST,
  WorkerId.JSON_PROPERTY_RPC_PORT,
  WorkerId.JSON_PROPERTY_PUSH_PORT,
  WorkerId.JSON_PROPERTY_FETCH_PORT,
  WorkerId.JSON_PROPERTY_REPLICATE_PORT
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class WorkerId {
  public static final String JSON_PROPERTY_HOST = "host";
  private String host;

  public static final String JSON_PROPERTY_RPC_PORT = "rpcPort";
  private Integer rpcPort;

  public static final String JSON_PROPERTY_PUSH_PORT = "pushPort";
  private Integer pushPort;

  public static final String JSON_PROPERTY_FETCH_PORT = "fetchPort";
  private Integer fetchPort;

  public static final String JSON_PROPERTY_REPLICATE_PORT = "replicatePort";
  private Integer replicatePort;

  public WorkerId() {
  }

  public WorkerId host(String host) {
    
    this.host = host;
    return this;
  }

  /**
   * The host of the worker.
   * @return host
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_HOST)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getHost() {
    return host;
  }


  @JsonProperty(JSON_PROPERTY_HOST)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setHost(String host) {
    this.host = host;
  }

  public WorkerId rpcPort(Integer rpcPort) {
    
    this.rpcPort = rpcPort;
    return this;
  }

  /**
   * The rpc port of the worker.
   * @return rpcPort
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_RPC_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Integer getRpcPort() {
    return rpcPort;
  }


  @JsonProperty(JSON_PROPERTY_RPC_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setRpcPort(Integer rpcPort) {
    this.rpcPort = rpcPort;
  }

  public WorkerId pushPort(Integer pushPort) {
    
    this.pushPort = pushPort;
    return this;
  }

  /**
   * The push port of the worker.
   * @return pushPort
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_PUSH_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Integer getPushPort() {
    return pushPort;
  }


  @JsonProperty(JSON_PROPERTY_PUSH_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setPushPort(Integer pushPort) {
    this.pushPort = pushPort;
  }

  public WorkerId fetchPort(Integer fetchPort) {
    
    this.fetchPort = fetchPort;
    return this;
  }

  /**
   * The fetch port of the worker.
   * @return fetchPort
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_FETCH_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Integer getFetchPort() {
    return fetchPort;
  }


  @JsonProperty(JSON_PROPERTY_FETCH_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setFetchPort(Integer fetchPort) {
    this.fetchPort = fetchPort;
  }

  public WorkerId replicatePort(Integer replicatePort) {
    
    this.replicatePort = replicatePort;
    return this;
  }

  /**
   * The replicate port of the worker.
   * @return replicatePort
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_REPLICATE_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Integer getReplicatePort() {
    return replicatePort;
  }


  @JsonProperty(JSON_PROPERTY_REPLICATE_PORT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setReplicatePort(Integer replicatePort) {
    this.replicatePort = replicatePort;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerId workerId = (WorkerId) o;
    return Objects.equals(this.host, workerId.host) &&
        Objects.equals(this.rpcPort, workerId.rpcPort) &&
        Objects.equals(this.pushPort, workerId.pushPort) &&
        Objects.equals(this.fetchPort, workerId.fetchPort) &&
        Objects.equals(this.replicatePort, workerId.replicatePort);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, rpcPort, pushPort, fetchPort, replicatePort);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class WorkerId {\n");
    sb.append("    host: ").append(toIndentedString(host)).append("\n");
    sb.append("    rpcPort: ").append(toIndentedString(rpcPort)).append("\n");
    sb.append("    pushPort: ").append(toIndentedString(pushPort)).append("\n");
    sb.append("    fetchPort: ").append(toIndentedString(fetchPort)).append("\n");
    sb.append("    replicatePort: ").append(toIndentedString(replicatePort)).append("\n");
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

