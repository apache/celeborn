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
 * WorkerResourceConsumption
 */
@JsonPropertyOrder({
  WorkerResourceConsumption.JSON_PROPERTY_DISK_BYTES_WRITTEN,
  WorkerResourceConsumption.JSON_PROPERTY_DISK_FILE_COUNT,
  WorkerResourceConsumption.JSON_PROPERTY_HDFS_BYTES_WRITTEN,
  WorkerResourceConsumption.JSON_PROPERTY_HDFS_FILE_COUNT,
  WorkerResourceConsumption.JSON_PROPERTY_SUB_RESOURCE_CONSUMPTIONS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class WorkerResourceConsumption {
  public static final String JSON_PROPERTY_DISK_BYTES_WRITTEN = "diskBytesWritten";
  @javax.annotation.Nullable
  private Long diskBytesWritten;

  public static final String JSON_PROPERTY_DISK_FILE_COUNT = "diskFileCount";
  @javax.annotation.Nullable
  private Long diskFileCount;

  public static final String JSON_PROPERTY_HDFS_BYTES_WRITTEN = "hdfsBytesWritten";
  @javax.annotation.Nullable
  private Long hdfsBytesWritten;

  public static final String JSON_PROPERTY_HDFS_FILE_COUNT = "hdfsFileCount";
  @javax.annotation.Nullable
  private Long hdfsFileCount;

  public static final String JSON_PROPERTY_SUB_RESOURCE_CONSUMPTIONS = "subResourceConsumptions";
  @javax.annotation.Nullable
  private Map<String, WorkerResourceConsumption> subResourceConsumptions = new HashMap<>();

  public WorkerResourceConsumption() {
  }

  public WorkerResourceConsumption diskBytesWritten(@javax.annotation.Nullable Long diskBytesWritten) {
    
    this.diskBytesWritten = diskBytesWritten;
    return this;
  }

  /**
   * Get diskBytesWritten
   * @return diskBytesWritten
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_DISK_BYTES_WRITTEN)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getDiskBytesWritten() {
    return diskBytesWritten;
  }


  @JsonProperty(JSON_PROPERTY_DISK_BYTES_WRITTEN)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setDiskBytesWritten(@javax.annotation.Nullable Long diskBytesWritten) {
    this.diskBytesWritten = diskBytesWritten;
  }

  public WorkerResourceConsumption diskFileCount(@javax.annotation.Nullable Long diskFileCount) {
    
    this.diskFileCount = diskFileCount;
    return this;
  }

  /**
   * Get diskFileCount
   * @return diskFileCount
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_DISK_FILE_COUNT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getDiskFileCount() {
    return diskFileCount;
  }


  @JsonProperty(JSON_PROPERTY_DISK_FILE_COUNT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setDiskFileCount(@javax.annotation.Nullable Long diskFileCount) {
    this.diskFileCount = diskFileCount;
  }

  public WorkerResourceConsumption hdfsBytesWritten(@javax.annotation.Nullable Long hdfsBytesWritten) {
    
    this.hdfsBytesWritten = hdfsBytesWritten;
    return this;
  }

  /**
   * Get hdfsBytesWritten
   * @return hdfsBytesWritten
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_HDFS_BYTES_WRITTEN)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getHdfsBytesWritten() {
    return hdfsBytesWritten;
  }


  @JsonProperty(JSON_PROPERTY_HDFS_BYTES_WRITTEN)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setHdfsBytesWritten(@javax.annotation.Nullable Long hdfsBytesWritten) {
    this.hdfsBytesWritten = hdfsBytesWritten;
  }

  public WorkerResourceConsumption hdfsFileCount(@javax.annotation.Nullable Long hdfsFileCount) {
    
    this.hdfsFileCount = hdfsFileCount;
    return this;
  }

  /**
   * Get hdfsFileCount
   * @return hdfsFileCount
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_HDFS_FILE_COUNT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getHdfsFileCount() {
    return hdfsFileCount;
  }


  @JsonProperty(JSON_PROPERTY_HDFS_FILE_COUNT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setHdfsFileCount(@javax.annotation.Nullable Long hdfsFileCount) {
    this.hdfsFileCount = hdfsFileCount;
  }

  public WorkerResourceConsumption subResourceConsumptions(@javax.annotation.Nullable Map<String, WorkerResourceConsumption> subResourceConsumptions) {
    
    this.subResourceConsumptions = subResourceConsumptions;
    return this;
  }

  public WorkerResourceConsumption putSubResourceConsumptionsItem(String key, WorkerResourceConsumption subResourceConsumptionsItem) {
    if (this.subResourceConsumptions == null) {
      this.subResourceConsumptions = new HashMap<>();
    }
    this.subResourceConsumptions.put(key, subResourceConsumptionsItem);
    return this;
  }

  /**
   * Get subResourceConsumptions
   * @return subResourceConsumptions
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_SUB_RESOURCE_CONSUMPTIONS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Map<String, WorkerResourceConsumption> getSubResourceConsumptions() {
    return subResourceConsumptions;
  }


  @JsonProperty(JSON_PROPERTY_SUB_RESOURCE_CONSUMPTIONS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setSubResourceConsumptions(@javax.annotation.Nullable Map<String, WorkerResourceConsumption> subResourceConsumptions) {
    this.subResourceConsumptions = subResourceConsumptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerResourceConsumption workerResourceConsumption = (WorkerResourceConsumption) o;
    return Objects.equals(this.diskBytesWritten, workerResourceConsumption.diskBytesWritten) &&
        Objects.equals(this.diskFileCount, workerResourceConsumption.diskFileCount) &&
        Objects.equals(this.hdfsBytesWritten, workerResourceConsumption.hdfsBytesWritten) &&
        Objects.equals(this.hdfsFileCount, workerResourceConsumption.hdfsFileCount) &&
        Objects.equals(this.subResourceConsumptions, workerResourceConsumption.subResourceConsumptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(diskBytesWritten, diskFileCount, hdfsBytesWritten, hdfsFileCount, subResourceConsumptions);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class WorkerResourceConsumption {\n");
    sb.append("    diskBytesWritten: ").append(toIndentedString(diskBytesWritten)).append("\n");
    sb.append("    diskFileCount: ").append(toIndentedString(diskFileCount)).append("\n");
    sb.append("    hdfsBytesWritten: ").append(toIndentedString(hdfsBytesWritten)).append("\n");
    sb.append("    hdfsFileCount: ").append(toIndentedString(hdfsFileCount)).append("\n");
    sb.append("    subResourceConsumptions: ").append(toIndentedString(subResourceConsumptions)).append("\n");
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

