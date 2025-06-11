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
import org.apache.celeborn.rest.v1.model.WorkerId;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * ExcludeWorkerRequest
 */
@JsonPropertyOrder({
  ExcludeWorkerRequest.JSON_PROPERTY_ADD,
  ExcludeWorkerRequest.JSON_PROPERTY_REMOVE
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class ExcludeWorkerRequest {
  public static final String JSON_PROPERTY_ADD = "add";
  @javax.annotation.Nullable
  private List<WorkerId> add = new ArrayList<>();

  public static final String JSON_PROPERTY_REMOVE = "remove";
  @javax.annotation.Nullable
  private List<WorkerId> remove = new ArrayList<>();

  public ExcludeWorkerRequest() {
  }

  public ExcludeWorkerRequest add(@javax.annotation.Nullable List<WorkerId> add) {
    
    this.add = add;
    return this;
  }

  public ExcludeWorkerRequest addAddItem(WorkerId addItem) {
    if (this.add == null) {
      this.add = new ArrayList<>();
    }
    this.add.add(addItem);
    return this;
  }

  /**
   * The workers to be added to the excluded workers.
   * @return add
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_ADD)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<WorkerId> getAdd() {
    return add;
  }


  @JsonProperty(JSON_PROPERTY_ADD)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setAdd(@javax.annotation.Nullable List<WorkerId> add) {
    this.add = add;
  }

  public ExcludeWorkerRequest remove(@javax.annotation.Nullable List<WorkerId> remove) {
    
    this.remove = remove;
    return this;
  }

  public ExcludeWorkerRequest addRemoveItem(WorkerId removeItem) {
    if (this.remove == null) {
      this.remove = new ArrayList<>();
    }
    this.remove.add(removeItem);
    return this;
  }

  /**
   * The workers to be removed from the excluded workers.
   * @return remove
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_REMOVE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<WorkerId> getRemove() {
    return remove;
  }


  @JsonProperty(JSON_PROPERTY_REMOVE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setRemove(@javax.annotation.Nullable List<WorkerId> remove) {
    this.remove = remove;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExcludeWorkerRequest excludeWorkerRequest = (ExcludeWorkerRequest) o;
    return Objects.equals(this.add, excludeWorkerRequest.add) &&
        Objects.equals(this.remove, excludeWorkerRequest.remove);
  }

  @Override
  public int hashCode() {
    return Objects.hash(add, remove);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ExcludeWorkerRequest {\n");
    sb.append("    add: ").append(toIndentedString(add)).append("\n");
    sb.append("    remove: ").append(toIndentedString(remove)).append("\n");
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

