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
 * RatisLogTermIndex
 */
@JsonPropertyOrder({
  RatisLogTermIndex.JSON_PROPERTY_TERM,
  RatisLogTermIndex.JSON_PROPERTY_INDEX
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class RatisLogTermIndex {
  public static final String JSON_PROPERTY_TERM = "term";
  @javax.annotation.Nonnull
  private Long term;

  public static final String JSON_PROPERTY_INDEX = "index";
  @javax.annotation.Nonnull
  private Long index;

  public RatisLogTermIndex() {
  }

  public RatisLogTermIndex term(@javax.annotation.Nonnull Long term) {
    
    this.term = term;
    return this;
  }

  /**
   * The term of the log entry.
   * @return term
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_TERM)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Long getTerm() {
    return term;
  }


  @JsonProperty(JSON_PROPERTY_TERM)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setTerm(@javax.annotation.Nonnull Long term) {
    this.term = term;
  }

  public RatisLogTermIndex index(@javax.annotation.Nonnull Long index) {
    
    this.index = index;
    return this;
  }

  /**
   * The index of the log entry.
   * @return index
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_INDEX)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Long getIndex() {
    return index;
  }


  @JsonProperty(JSON_PROPERTY_INDEX)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setIndex(@javax.annotation.Nonnull Long index) {
    this.index = index;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RatisLogTermIndex ratisLogTermIndex = (RatisLogTermIndex) o;
    return Objects.equals(this.term, ratisLogTermIndex.term) &&
        Objects.equals(this.index, ratisLogTermIndex.index);
  }

  @Override
  public int hashCode() {
    return Objects.hash(term, index);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class RatisLogTermIndex {\n");
    sb.append("    term: ").append(toIndentedString(term)).append("\n");
    sb.append("    index: ").append(toIndentedString(index)).append("\n");
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

