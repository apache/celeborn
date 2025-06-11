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
 * PartitionLocationData
 */
@JsonPropertyOrder({
  PartitionLocationData.JSON_PROPERTY_ID_EPOCH,
  PartitionLocationData.JSON_PROPERTY_HOST_AND_PORTS,
  PartitionLocationData.JSON_PROPERTY_MODE,
  PartitionLocationData.JSON_PROPERTY_PEER,
  PartitionLocationData.JSON_PROPERTY_STORAGE,
  PartitionLocationData.JSON_PROPERTY_MAP_ID_BIT_MAP
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class PartitionLocationData {
  public static final String JSON_PROPERTY_ID_EPOCH = "idEpoch";
  @javax.annotation.Nullable
  private String idEpoch;

  public static final String JSON_PROPERTY_HOST_AND_PORTS = "hostAndPorts";
  @javax.annotation.Nullable
  private String hostAndPorts;

  /**
   * partition mode.
   */
  public enum ModeEnum {
    PRIMARY(String.valueOf("PRIMARY")),
    
    REPLICA(String.valueOf("REPLICA"));

    private String value;

    ModeEnum(String value) {
      this.value = value;
    }

    @JsonValue
    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static ModeEnum fromValue(String value) {
      for (ModeEnum b : ModeEnum.values()) {
        if (b.value.equalsIgnoreCase(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  public static final String JSON_PROPERTY_MODE = "mode";
  @javax.annotation.Nullable
  private ModeEnum mode;

  public static final String JSON_PROPERTY_PEER = "peer";
  @javax.annotation.Nullable
  private String peer;

  /**
   * The storage hint.
   */
  public enum StorageEnum {
    MEMORY(String.valueOf("MEMORY")),
    
    HDD(String.valueOf("HDD")),
    
    SSD(String.valueOf("SSD")),
    
    HDFS(String.valueOf("HDFS")),
    
    OSS(String.valueOf("OSS")),
    
    S3(String.valueOf("S3"));

    private String value;

    StorageEnum(String value) {
      this.value = value;
    }

    @JsonValue
    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static StorageEnum fromValue(String value) {
      for (StorageEnum b : StorageEnum.values()) {
        if (b.value.equalsIgnoreCase(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  public static final String JSON_PROPERTY_STORAGE = "storage";
  @javax.annotation.Nullable
  private StorageEnum storage;

  public static final String JSON_PROPERTY_MAP_ID_BIT_MAP = "mapIdBitMap";
  @javax.annotation.Nullable
  private String mapIdBitMap;

  public PartitionLocationData() {
  }

  public PartitionLocationData idEpoch(@javax.annotation.Nullable String idEpoch) {
    
    this.idEpoch = idEpoch;
    return this;
  }

  /**
   * The id and epoch.
   * @return idEpoch
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_ID_EPOCH)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getIdEpoch() {
    return idEpoch;
  }


  @JsonProperty(JSON_PROPERTY_ID_EPOCH)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setIdEpoch(@javax.annotation.Nullable String idEpoch) {
    this.idEpoch = idEpoch;
  }

  public PartitionLocationData hostAndPorts(@javax.annotation.Nullable String hostAndPorts) {
    
    this.hostAndPorts = hostAndPorts;
    return this;
  }

  /**
   * The host-rpcPort-pushPort-fetchPort-replicatePort.
   * @return hostAndPorts
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_HOST_AND_PORTS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getHostAndPorts() {
    return hostAndPorts;
  }


  @JsonProperty(JSON_PROPERTY_HOST_AND_PORTS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setHostAndPorts(@javax.annotation.Nullable String hostAndPorts) {
    this.hostAndPorts = hostAndPorts;
  }

  public PartitionLocationData mode(@javax.annotation.Nullable ModeEnum mode) {
    
    this.mode = mode;
    return this;
  }

  /**
   * partition mode.
   * @return mode
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_MODE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public ModeEnum getMode() {
    return mode;
  }


  @JsonProperty(JSON_PROPERTY_MODE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setMode(@javax.annotation.Nullable ModeEnum mode) {
    this.mode = mode;
  }

  public PartitionLocationData peer(@javax.annotation.Nullable String peer) {
    
    this.peer = peer;
    return this;
  }

  /**
   * The peer address.
   * @return peer
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_PEER)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getPeer() {
    return peer;
  }


  @JsonProperty(JSON_PROPERTY_PEER)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setPeer(@javax.annotation.Nullable String peer) {
    this.peer = peer;
  }

  public PartitionLocationData storage(@javax.annotation.Nullable StorageEnum storage) {
    
    this.storage = storage;
    return this;
  }

  /**
   * The storage hint.
   * @return storage
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_STORAGE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public StorageEnum getStorage() {
    return storage;
  }


  @JsonProperty(JSON_PROPERTY_STORAGE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setStorage(@javax.annotation.Nullable StorageEnum storage) {
    this.storage = storage;
  }

  public PartitionLocationData mapIdBitMap(@javax.annotation.Nullable String mapIdBitMap) {
    
    this.mapIdBitMap = mapIdBitMap;
    return this;
  }

  /**
   * The map id bitmap hint.
   * @return mapIdBitMap
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_MAP_ID_BIT_MAP)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getMapIdBitMap() {
    return mapIdBitMap;
  }


  @JsonProperty(JSON_PROPERTY_MAP_ID_BIT_MAP)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setMapIdBitMap(@javax.annotation.Nullable String mapIdBitMap) {
    this.mapIdBitMap = mapIdBitMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionLocationData partitionLocationData = (PartitionLocationData) o;
    return Objects.equals(this.idEpoch, partitionLocationData.idEpoch) &&
        Objects.equals(this.hostAndPorts, partitionLocationData.hostAndPorts) &&
        Objects.equals(this.mode, partitionLocationData.mode) &&
        Objects.equals(this.peer, partitionLocationData.peer) &&
        Objects.equals(this.storage, partitionLocationData.storage) &&
        Objects.equals(this.mapIdBitMap, partitionLocationData.mapIdBitMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(idEpoch, hostAndPorts, mode, peer, storage, mapIdBitMap);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class PartitionLocationData {\n");
    sb.append("    idEpoch: ").append(toIndentedString(idEpoch)).append("\n");
    sb.append("    hostAndPorts: ").append(toIndentedString(hostAndPorts)).append("\n");
    sb.append("    mode: ").append(toIndentedString(mode)).append("\n");
    sb.append("    peer: ").append(toIndentedString(peer)).append("\n");
    sb.append("    storage: ").append(toIndentedString(storage)).append("\n");
    sb.append("    mapIdBitMap: ").append(toIndentedString(mapIdBitMap)).append("\n");
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

