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
import org.apache.celeborn.rest.v1.model.ThreadStack;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * ThreadStackResponse
 */
@JsonPropertyOrder({
  ThreadStackResponse.JSON_PROPERTY_THREAD_STACKS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class ThreadStackResponse {
  public static final String JSON_PROPERTY_THREAD_STACKS = "threadStacks";
  private List<ThreadStack> threadStacks = new ArrayList<>();

  public ThreadStackResponse() {
  }

  public ThreadStackResponse threadStacks(List<ThreadStack> threadStacks) {
    
    this.threadStacks = threadStacks;
    return this;
  }

  public ThreadStackResponse addThreadStacksItem(ThreadStack threadStacksItem) {
    if (this.threadStacks == null) {
      this.threadStacks = new ArrayList<>();
    }
    this.threadStacks.add(threadStacksItem);
    return this;
  }

  /**
   * The thread stacks.
   * @return threadStacks
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_THREAD_STACKS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public List<ThreadStack> getThreadStacks() {
    return threadStacks;
  }


  @JsonProperty(JSON_PROPERTY_THREAD_STACKS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setThreadStacks(List<ThreadStack> threadStacks) {
    this.threadStacks = threadStacks;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ThreadStackResponse threadStackResponse = (ThreadStackResponse) o;
    return Objects.equals(this.threadStacks, threadStackResponse.threadStacks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(threadStacks);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ThreadStackResponse {\n");
    sb.append("    threadStacks: ").append(toIndentedString(threadStacks)).append("\n");
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

