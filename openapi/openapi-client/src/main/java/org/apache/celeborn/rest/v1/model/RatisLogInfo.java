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
import org.apache.celeborn.rest.v1.model.RatisLogTermIndex;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * RatisLogInfo
 */
@JsonPropertyOrder({
  RatisLogInfo.JSON_PROPERTY_LAST_SNAPSHOT,
  RatisLogInfo.JSON_PROPERTY_APPLIED,
  RatisLogInfo.JSON_PROPERTY_COMMITTED,
  RatisLogInfo.JSON_PROPERTY_LAST_ENTRY
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class RatisLogInfo {
  public static final String JSON_PROPERTY_LAST_SNAPSHOT = "lastSnapshot";
  @javax.annotation.Nullable
  private RatisLogTermIndex lastSnapshot;

  public static final String JSON_PROPERTY_APPLIED = "applied";
  @javax.annotation.Nullable
  private RatisLogTermIndex applied;

  public static final String JSON_PROPERTY_COMMITTED = "committed";
  @javax.annotation.Nullable
  private RatisLogTermIndex committed;

  public static final String JSON_PROPERTY_LAST_ENTRY = "lastEntry";
  @javax.annotation.Nullable
  private RatisLogTermIndex lastEntry;

  public RatisLogInfo() {
  }

  public RatisLogInfo lastSnapshot(@javax.annotation.Nullable RatisLogTermIndex lastSnapshot) {
    
    this.lastSnapshot = lastSnapshot;
    return this;
  }

  /**
   * Get lastSnapshot
   * @return lastSnapshot
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_LAST_SNAPSHOT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public RatisLogTermIndex getLastSnapshot() {
    return lastSnapshot;
  }


  @JsonProperty(JSON_PROPERTY_LAST_SNAPSHOT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setLastSnapshot(@javax.annotation.Nullable RatisLogTermIndex lastSnapshot) {
    this.lastSnapshot = lastSnapshot;
  }

  public RatisLogInfo applied(@javax.annotation.Nullable RatisLogTermIndex applied) {
    
    this.applied = applied;
    return this;
  }

  /**
   * Get applied
   * @return applied
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_APPLIED)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public RatisLogTermIndex getApplied() {
    return applied;
  }


  @JsonProperty(JSON_PROPERTY_APPLIED)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setApplied(@javax.annotation.Nullable RatisLogTermIndex applied) {
    this.applied = applied;
  }

  public RatisLogInfo committed(@javax.annotation.Nullable RatisLogTermIndex committed) {
    
    this.committed = committed;
    return this;
  }

  /**
   * Get committed
   * @return committed
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_COMMITTED)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public RatisLogTermIndex getCommitted() {
    return committed;
  }


  @JsonProperty(JSON_PROPERTY_COMMITTED)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setCommitted(@javax.annotation.Nullable RatisLogTermIndex committed) {
    this.committed = committed;
  }

  public RatisLogInfo lastEntry(@javax.annotation.Nullable RatisLogTermIndex lastEntry) {
    
    this.lastEntry = lastEntry;
    return this;
  }

  /**
   * Get lastEntry
   * @return lastEntry
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_LAST_ENTRY)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public RatisLogTermIndex getLastEntry() {
    return lastEntry;
  }


  @JsonProperty(JSON_PROPERTY_LAST_ENTRY)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setLastEntry(@javax.annotation.Nullable RatisLogTermIndex lastEntry) {
    this.lastEntry = lastEntry;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RatisLogInfo ratisLogInfo = (RatisLogInfo) o;
    return Objects.equals(this.lastSnapshot, ratisLogInfo.lastSnapshot) &&
        Objects.equals(this.applied, ratisLogInfo.applied) &&
        Objects.equals(this.committed, ratisLogInfo.committed) &&
        Objects.equals(this.lastEntry, ratisLogInfo.lastEntry);
  }

  @Override
  public int hashCode() {
    return Objects.hash(lastSnapshot, applied, committed, lastEntry);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class RatisLogInfo {\n");
    sb.append("    lastSnapshot: ").append(toIndentedString(lastSnapshot)).append("\n");
    sb.append("    applied: ").append(toIndentedString(applied)).append("\n");
    sb.append("    committed: ").append(toIndentedString(committed)).append("\n");
    sb.append("    lastEntry: ").append(toIndentedString(lastEntry)).append("\n");
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

