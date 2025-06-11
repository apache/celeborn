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
import org.apache.celeborn.rest.v1.model.WorkerId;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * WorkerInterruptionNotice
 */
@JsonPropertyOrder({
  WorkerInterruptionNotice.JSON_PROPERTY_WORKER_ID,
  WorkerInterruptionNotice.JSON_PROPERTY_INTERRUPTION_TIMESTAMP
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class WorkerInterruptionNotice {
  public static final String JSON_PROPERTY_WORKER_ID = "workerId";
  @javax.annotation.Nonnull
  private WorkerId workerId;

  public static final String JSON_PROPERTY_INTERRUPTION_TIMESTAMP = "interruptionTimestamp";
  @javax.annotation.Nonnull
  private Long interruptionTimestamp;

  public WorkerInterruptionNotice() {
  }

  public WorkerInterruptionNotice workerId(@javax.annotation.Nonnull WorkerId workerId) {
    
    this.workerId = workerId;
    return this;
  }

  /**
   * Get workerId
   * @return workerId
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_WORKER_ID)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public WorkerId getWorkerId() {
    return workerId;
  }


  @JsonProperty(JSON_PROPERTY_WORKER_ID)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setWorkerId(@javax.annotation.Nonnull WorkerId workerId) {
    this.workerId = workerId;
  }

  public WorkerInterruptionNotice interruptionTimestamp(@javax.annotation.Nonnull Long interruptionTimestamp) {
    
    this.interruptionTimestamp = interruptionTimestamp;
    return this;
  }

  /**
   * The datetime of the expected interruption.
   * @return interruptionTimestamp
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_INTERRUPTION_TIMESTAMP)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Long getInterruptionTimestamp() {
    return interruptionTimestamp;
  }


  @JsonProperty(JSON_PROPERTY_INTERRUPTION_TIMESTAMP)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setInterruptionTimestamp(@javax.annotation.Nonnull Long interruptionTimestamp) {
    this.interruptionTimestamp = interruptionTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WorkerInterruptionNotice workerInterruptionNotice = (WorkerInterruptionNotice) o;
    return Objects.equals(this.workerId, workerInterruptionNotice.workerId) &&
        Objects.equals(this.interruptionTimestamp, workerInterruptionNotice.interruptionTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(workerId, interruptionTimestamp);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class WorkerInterruptionNotice {\n");
    sb.append("    workerId: ").append(toIndentedString(workerId)).append("\n");
    sb.append("    interruptionTimestamp: ").append(toIndentedString(interruptionTimestamp)).append("\n");
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

