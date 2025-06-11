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
import org.apache.celeborn.rest.v1.model.WorkerEventData;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * WorkerEventsResponse
 */
@JsonPropertyOrder({
  WorkerEventsResponse.JSON_PROPERTY_WORKER_EVENTS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class WorkerEventsResponse {
  public static final String JSON_PROPERTY_WORKER_EVENTS = "workerEvents";
  @javax.annotation.Nullable
  private List<WorkerEventData> workerEvents = new ArrayList<>();

  public WorkerEventsResponse() {
  }

  public WorkerEventsResponse workerEvents(@javax.annotation.Nullable List<WorkerEventData> workerEvents) {
    
    this.workerEvents = workerEvents;
    return this;
  }

  public WorkerEventsResponse addWorkerEventsItem(WorkerEventData workerEventsItem) {
    if (this.workerEvents == null) {
      this.workerEvents = new ArrayList<>();
    }
    this.workerEvents.add(workerEventsItem);
    return this;
  }

  /**
   * The worker events.
   * @return workerEvents
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_WORKER_EVENTS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<WorkerEventData> getWorkerEvents() {
    return workerEvents;
  }


  @JsonProperty(JSON_PROPERTY_WORKER_EVENTS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setWorkerEvents(@javax.annotation.Nullable List<WorkerEventData> workerEvents) {
    this.workerEvents = workerEvents;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerEventsResponse workerEventsResponse = (WorkerEventsResponse) o;
    return Objects.equals(this.workerEvents, workerEventsResponse.workerEvents);
  }

  @Override
  public int hashCode() {
    return Objects.hash(workerEvents);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class WorkerEventsResponse {\n");
    sb.append("    workerEvents: ").append(toIndentedString(workerEvents)).append("\n");
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

