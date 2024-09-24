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
import org.apache.celeborn.rest.v1.model.PartitionLocationData;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * ShufflePartitionsResponse
 */
@JsonPropertyOrder({
  ShufflePartitionsResponse.JSON_PROPERTY_PRIMARY_PARTITIONS,
  ShufflePartitionsResponse.JSON_PROPERTY_REPLICA_PARTITIONS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class ShufflePartitionsResponse {
  public static final String JSON_PROPERTY_PRIMARY_PARTITIONS = "primaryPartitions";
  private Map<String, Map<String, PartitionLocationData>> primaryPartitions = new HashMap<>();

  public static final String JSON_PROPERTY_REPLICA_PARTITIONS = "replicaPartitions";
  private Map<String, Map<String, PartitionLocationData>> replicaPartitions = new HashMap<>();

  public ShufflePartitionsResponse() {
  }

  public ShufflePartitionsResponse primaryPartitions(Map<String, Map<String, PartitionLocationData>> primaryPartitions) {
    
    this.primaryPartitions = primaryPartitions;
    return this;
  }

  public ShufflePartitionsResponse putPrimaryPartitionsItem(String key, Map<String, PartitionLocationData> primaryPartitionsItem) {
    if (this.primaryPartitions == null) {
      this.primaryPartitions = new HashMap<>();
    }
    this.primaryPartitions.put(key, primaryPartitionsItem);
    return this;
  }

  /**
   * Get primaryPartitions
   * @return primaryPartitions
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_PRIMARY_PARTITIONS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Map<String, Map<String, PartitionLocationData>> getPrimaryPartitions() {
    return primaryPartitions;
  }


  @JsonProperty(JSON_PROPERTY_PRIMARY_PARTITIONS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setPrimaryPartitions(Map<String, Map<String, PartitionLocationData>> primaryPartitions) {
    this.primaryPartitions = primaryPartitions;
  }

  public ShufflePartitionsResponse replicaPartitions(Map<String, Map<String, PartitionLocationData>> replicaPartitions) {
    
    this.replicaPartitions = replicaPartitions;
    return this;
  }

  public ShufflePartitionsResponse putReplicaPartitionsItem(String key, Map<String, PartitionLocationData> replicaPartitionsItem) {
    if (this.replicaPartitions == null) {
      this.replicaPartitions = new HashMap<>();
    }
    this.replicaPartitions.put(key, replicaPartitionsItem);
    return this;
  }

  /**
   * Get replicaPartitions
   * @return replicaPartitions
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_REPLICA_PARTITIONS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Map<String, Map<String, PartitionLocationData>> getReplicaPartitions() {
    return replicaPartitions;
  }


  @JsonProperty(JSON_PROPERTY_REPLICA_PARTITIONS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setReplicaPartitions(Map<String, Map<String, PartitionLocationData>> replicaPartitions) {
    this.replicaPartitions = replicaPartitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShufflePartitionsResponse shufflePartitionsResponse = (ShufflePartitionsResponse) o;
    return Objects.equals(this.primaryPartitions, shufflePartitionsResponse.primaryPartitions) &&
        Objects.equals(this.replicaPartitions, shufflePartitionsResponse.replicaPartitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(primaryPartitions, replicaPartitions);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ShufflePartitionsResponse {\n");
    sb.append("    primaryPartitions: ").append(toIndentedString(primaryPartitions)).append("\n");
    sb.append("    replicaPartitions: ").append(toIndentedString(replicaPartitions)).append("\n");
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

