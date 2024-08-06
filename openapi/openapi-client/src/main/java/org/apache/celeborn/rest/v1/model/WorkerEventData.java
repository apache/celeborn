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
import org.apache.celeborn.rest.v1.model.WorkerEventInfoData;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * WorkerEventData
 */
@JsonPropertyOrder({
  WorkerEventData.JSON_PROPERTY_WORKER,
  WorkerEventData.JSON_PROPERTY_EVENT
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.7.0")
public class WorkerEventData {
  public static final String JSON_PROPERTY_WORKER = "worker";
  private WorkerData worker;

  public static final String JSON_PROPERTY_EVENT = "event";
  private WorkerEventInfoData event;

  public WorkerEventData() {
  }

  public WorkerEventData worker(WorkerData worker) {
    
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
  public void setWorker(WorkerData worker) {
    this.worker = worker;
  }

  public WorkerEventData event(WorkerEventInfoData event) {
    
    this.event = event;
    return this;
  }

  /**
   * Get event
   * @return event
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_EVENT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public WorkerEventInfoData getEvent() {
    return event;
  }


  @JsonProperty(JSON_PROPERTY_EVENT)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setEvent(WorkerEventInfoData event) {
    this.event = event;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerEventData workerEventData = (WorkerEventData) o;
    return Objects.equals(this.worker, workerEventData.worker) &&
        Objects.equals(this.event, workerEventData.event);
  }

  @Override
  public int hashCode() {
    return Objects.hash(worker, event);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class WorkerEventData {\n");
    sb.append("    worker: ").append(toIndentedString(worker)).append("\n");
    sb.append("    event: ").append(toIndentedString(event)).append("\n");
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

