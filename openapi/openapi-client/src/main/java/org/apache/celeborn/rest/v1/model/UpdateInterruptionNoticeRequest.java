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
import org.apache.celeborn.rest.v1.model.WorkerInterruptionNotice;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * UpdateInterruptionNoticeRequest
 */
@JsonPropertyOrder({
  UpdateInterruptionNoticeRequest.JSON_PROPERTY_WORKERS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class UpdateInterruptionNoticeRequest {
  public static final String JSON_PROPERTY_WORKERS = "workers";
  @javax.annotation.Nullable
  private List<WorkerInterruptionNotice> workers = new ArrayList<>();

  public UpdateInterruptionNoticeRequest() {
  }

  public UpdateInterruptionNoticeRequest workers(@javax.annotation.Nullable List<WorkerInterruptionNotice> workers) {
    
    this.workers = workers;
    return this;
  }

  public UpdateInterruptionNoticeRequest addWorkersItem(WorkerInterruptionNotice workersItem) {
    if (this.workers == null) {
      this.workers = new ArrayList<>();
    }
    this.workers.add(workersItem);
    return this;
  }

  /**
   * The workers to be removed from the master workers unavailable info.
   * @return workers
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<WorkerInterruptionNotice> getWorkers() {
    return workers;
  }


  @JsonProperty(JSON_PROPERTY_WORKERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setWorkers(@javax.annotation.Nullable List<WorkerInterruptionNotice> workers) {
    this.workers = workers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UpdateInterruptionNoticeRequest updateInterruptionNoticeRequest = (UpdateInterruptionNoticeRequest) o;
    return Objects.equals(this.workers, updateInterruptionNoticeRequest.workers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(workers);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class UpdateInterruptionNoticeRequest {\n");
    sb.append("    workers: ").append(toIndentedString(workers)).append("\n");
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

