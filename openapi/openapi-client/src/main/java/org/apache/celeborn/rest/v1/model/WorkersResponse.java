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
import org.apache.celeborn.rest.v1.model.WorkerData;
import org.apache.celeborn.rest.v1.model.WorkerTimestampData;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * WorkersResponse
 */
@JsonPropertyOrder({
  WorkersResponse.JSON_PROPERTY_WORKERS,
  WorkersResponse.JSON_PROPERTY_LOST_WORKERS,
  WorkersResponse.JSON_PROPERTY_EXCLUDED_WORKERS,
  WorkersResponse.JSON_PROPERTY_MANUAL_EXCLUDED_WORKERS,
  WorkersResponse.JSON_PROPERTY_SHUTDOWN_WORKERS,
  WorkersResponse.JSON_PROPERTY_DECOMMISSIONING_WORKERS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class WorkersResponse {
  public static final String JSON_PROPERTY_WORKERS = "workers";
  private List<WorkerData> workers = new ArrayList<>();

  public static final String JSON_PROPERTY_LOST_WORKERS = "lostWorkers";
  private List<WorkerTimestampData> lostWorkers = new ArrayList<>();

  public static final String JSON_PROPERTY_EXCLUDED_WORKERS = "excludedWorkers";
  private List<WorkerData> excludedWorkers = new ArrayList<>();

  public static final String JSON_PROPERTY_MANUAL_EXCLUDED_WORKERS = "manualExcludedWorkers";
  private List<WorkerData> manualExcludedWorkers = new ArrayList<>();

  public static final String JSON_PROPERTY_SHUTDOWN_WORKERS = "shutdownWorkers";
  private List<WorkerData> shutdownWorkers = new ArrayList<>();

  public static final String JSON_PROPERTY_DECOMMISSIONING_WORKERS = "decommissioningWorkers";
  private List<WorkerData> decommissioningWorkers = new ArrayList<>();

  public WorkersResponse() {
  }

  public WorkersResponse workers(List<WorkerData> workers) {
    
    this.workers = workers;
    return this;
  }

  public WorkersResponse addWorkersItem(WorkerData workersItem) {
    if (this.workers == null) {
      this.workers = new ArrayList<>();
    }
    this.workers.add(workersItem);
    return this;
  }

  /**
   * The registered workers.
   * @return workers
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_WORKERS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public List<WorkerData> getWorkers() {
    return workers;
  }


  @JsonProperty(JSON_PROPERTY_WORKERS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setWorkers(List<WorkerData> workers) {
    this.workers = workers;
  }

  public WorkersResponse lostWorkers(List<WorkerTimestampData> lostWorkers) {
    
    this.lostWorkers = lostWorkers;
    return this;
  }

  public WorkersResponse addLostWorkersItem(WorkerTimestampData lostWorkersItem) {
    if (this.lostWorkers == null) {
      this.lostWorkers = new ArrayList<>();
    }
    this.lostWorkers.add(lostWorkersItem);
    return this;
  }

  /**
   * The lost workers.
   * @return lostWorkers
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_LOST_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<WorkerTimestampData> getLostWorkers() {
    return lostWorkers;
  }


  @JsonProperty(JSON_PROPERTY_LOST_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setLostWorkers(List<WorkerTimestampData> lostWorkers) {
    this.lostWorkers = lostWorkers;
  }

  public WorkersResponse excludedWorkers(List<WorkerData> excludedWorkers) {
    
    this.excludedWorkers = excludedWorkers;
    return this;
  }

  public WorkersResponse addExcludedWorkersItem(WorkerData excludedWorkersItem) {
    if (this.excludedWorkers == null) {
      this.excludedWorkers = new ArrayList<>();
    }
    this.excludedWorkers.add(excludedWorkersItem);
    return this;
  }

  /**
   * The excluded workers.
   * @return excludedWorkers
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_EXCLUDED_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<WorkerData> getExcludedWorkers() {
    return excludedWorkers;
  }


  @JsonProperty(JSON_PROPERTY_EXCLUDED_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setExcludedWorkers(List<WorkerData> excludedWorkers) {
    this.excludedWorkers = excludedWorkers;
  }

  public WorkersResponse manualExcludedWorkers(List<WorkerData> manualExcludedWorkers) {
    
    this.manualExcludedWorkers = manualExcludedWorkers;
    return this;
  }

  public WorkersResponse addManualExcludedWorkersItem(WorkerData manualExcludedWorkersItem) {
    if (this.manualExcludedWorkers == null) {
      this.manualExcludedWorkers = new ArrayList<>();
    }
    this.manualExcludedWorkers.add(manualExcludedWorkersItem);
    return this;
  }

  /**
   * The manual excluded workers.
   * @return manualExcludedWorkers
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_MANUAL_EXCLUDED_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<WorkerData> getManualExcludedWorkers() {
    return manualExcludedWorkers;
  }


  @JsonProperty(JSON_PROPERTY_MANUAL_EXCLUDED_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setManualExcludedWorkers(List<WorkerData> manualExcludedWorkers) {
    this.manualExcludedWorkers = manualExcludedWorkers;
  }

  public WorkersResponse shutdownWorkers(List<WorkerData> shutdownWorkers) {
    
    this.shutdownWorkers = shutdownWorkers;
    return this;
  }

  public WorkersResponse addShutdownWorkersItem(WorkerData shutdownWorkersItem) {
    if (this.shutdownWorkers == null) {
      this.shutdownWorkers = new ArrayList<>();
    }
    this.shutdownWorkers.add(shutdownWorkersItem);
    return this;
  }

  /**
   * The shutdown workers.
   * @return shutdownWorkers
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_SHUTDOWN_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<WorkerData> getShutdownWorkers() {
    return shutdownWorkers;
  }


  @JsonProperty(JSON_PROPERTY_SHUTDOWN_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setShutdownWorkers(List<WorkerData> shutdownWorkers) {
    this.shutdownWorkers = shutdownWorkers;
  }

  public WorkersResponse decommissioningWorkers(List<WorkerData> decommissioningWorkers) {
    
    this.decommissioningWorkers = decommissioningWorkers;
    return this;
  }

  public WorkersResponse addDecommissioningWorkersItem(WorkerData decommissioningWorkersItem) {
    if (this.decommissioningWorkers == null) {
      this.decommissioningWorkers = new ArrayList<>();
    }
    this.decommissioningWorkers.add(decommissioningWorkersItem);
    return this;
  }

  /**
   * The decommissioning workers.
   * @return decommissioningWorkers
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_DECOMMISSIONING_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<WorkerData> getDecommissioningWorkers() {
    return decommissioningWorkers;
  }


  @JsonProperty(JSON_PROPERTY_DECOMMISSIONING_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setDecommissioningWorkers(List<WorkerData> decommissioningWorkers) {
    this.decommissioningWorkers = decommissioningWorkers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkersResponse workersResponse = (WorkersResponse) o;
    return Objects.equals(this.workers, workersResponse.workers) &&
        Objects.equals(this.lostWorkers, workersResponse.lostWorkers) &&
        Objects.equals(this.excludedWorkers, workersResponse.excludedWorkers) &&
        Objects.equals(this.manualExcludedWorkers, workersResponse.manualExcludedWorkers) &&
        Objects.equals(this.shutdownWorkers, workersResponse.shutdownWorkers) &&
        Objects.equals(this.decommissioningWorkers, workersResponse.decommissioningWorkers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(workers, lostWorkers, excludedWorkers, manualExcludedWorkers, shutdownWorkers, decommissioningWorkers);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class WorkersResponse {\n");
    sb.append("    workers: ").append(toIndentedString(workers)).append("\n");
    sb.append("    lostWorkers: ").append(toIndentedString(lostWorkers)).append("\n");
    sb.append("    excludedWorkers: ").append(toIndentedString(excludedWorkers)).append("\n");
    sb.append("    manualExcludedWorkers: ").append(toIndentedString(manualExcludedWorkers)).append("\n");
    sb.append("    shutdownWorkers: ").append(toIndentedString(shutdownWorkers)).append("\n");
    sb.append("    decommissioningWorkers: ").append(toIndentedString(decommissioningWorkers)).append("\n");
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

