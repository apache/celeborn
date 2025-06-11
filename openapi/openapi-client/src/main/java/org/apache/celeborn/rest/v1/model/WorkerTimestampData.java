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
import org.apache.celeborn.rest.v1.model.WorkerData;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * WorkerTimestampData
 */
@JsonPropertyOrder({
  WorkerTimestampData.JSON_PROPERTY_WORKER,
  WorkerTimestampData.JSON_PROPERTY_TIMESTAMP
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class WorkerTimestampData {
  public static final String JSON_PROPERTY_WORKER = "worker";
  @javax.annotation.Nonnull
  private WorkerData worker;

  public static final String JSON_PROPERTY_TIMESTAMP = "timestamp";
  @javax.annotation.Nonnull
  private Long timestamp;

  public WorkerTimestampData() {
  }

  public WorkerTimestampData worker(@javax.annotation.Nonnull WorkerData worker) {
    
    this.worker = worker;
    return this;
  }

  /**
   * Get worker
   * @return worker
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_WORKER)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public WorkerData getWorker() {
    return worker;
  }


  @JsonProperty(JSON_PROPERTY_WORKER)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setWorker(@javax.annotation.Nonnull WorkerData worker) {
    this.worker = worker;
  }

  public WorkerTimestampData timestamp(@javax.annotation.Nonnull Long timestamp) {
    
    this.timestamp = timestamp;
    return this;
  }

  /**
   * Get timestamp
   * @return timestamp
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_TIMESTAMP)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Long getTimestamp() {
    return timestamp;
  }


  @JsonProperty(JSON_PROPERTY_TIMESTAMP)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setTimestamp(@javax.annotation.Nonnull Long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerTimestampData workerTimestampData = (WorkerTimestampData) o;
    return Objects.equals(this.worker, workerTimestampData.worker) &&
        Objects.equals(this.timestamp, workerTimestampData.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(worker, timestamp);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class WorkerTimestampData {\n");
    sb.append("    worker: ").append(toIndentedString(worker)).append("\n");
    sb.append("    timestamp: ").append(toIndentedString(timestamp)).append("\n");
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

