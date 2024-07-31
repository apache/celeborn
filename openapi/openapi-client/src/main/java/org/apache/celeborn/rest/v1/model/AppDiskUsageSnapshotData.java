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
import org.apache.celeborn.rest.v1.model.AppDiskUsageData;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * AppDiskUsageSnapshotData
 */
@JsonPropertyOrder({
  AppDiskUsageSnapshotData.JSON_PROPERTY_START,
  AppDiskUsageSnapshotData.JSON_PROPERTY_END,
  AppDiskUsageSnapshotData.JSON_PROPERTY_TOP_N_ITEMS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.7.0")
public class AppDiskUsageSnapshotData {
  public static final String JSON_PROPERTY_START = "start";
  private Long start;

  public static final String JSON_PROPERTY_END = "end";
  private Long end;

  public static final String JSON_PROPERTY_TOP_N_ITEMS = "topNItems";
  private List<AppDiskUsageData> topNItems = new ArrayList<>();

  public AppDiskUsageSnapshotData() {
  }

  public AppDiskUsageSnapshotData start(Long start) {
    
    this.start = start;
    return this;
  }

  /**
   * The start timestamp of the snapshot.
   * @return start
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_START)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getStart() {
    return start;
  }


  @JsonProperty(JSON_PROPERTY_START)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setStart(Long start) {
    this.start = start;
  }

  public AppDiskUsageSnapshotData end(Long end) {
    
    this.end = end;
    return this;
  }

  /**
   * The end timestamp of the snapshot.
   * @return end
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_END)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getEnd() {
    return end;
  }


  @JsonProperty(JSON_PROPERTY_END)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setEnd(Long end) {
    this.end = end;
  }

  public AppDiskUsageSnapshotData topNItems(List<AppDiskUsageData> topNItems) {
    
    this.topNItems = topNItems;
    return this;
  }

  public AppDiskUsageSnapshotData addTopNItemsItem(AppDiskUsageData topNItemsItem) {
    if (this.topNItems == null) {
      this.topNItems = new ArrayList<>();
    }
    this.topNItems.add(topNItemsItem);
    return this;
  }

  /**
   * The top N app disk usages.
   * @return topNItems
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_TOP_N_ITEMS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<AppDiskUsageData> getTopNItems() {
    return topNItems;
  }


  @JsonProperty(JSON_PROPERTY_TOP_N_ITEMS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setTopNItems(List<AppDiskUsageData> topNItems) {
    this.topNItems = topNItems;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AppDiskUsageSnapshotData appDiskUsageSnapshotData = (AppDiskUsageSnapshotData) o;
    return Objects.equals(this.start, appDiskUsageSnapshotData.start) &&
        Objects.equals(this.end, appDiskUsageSnapshotData.end) &&
        Objects.equals(this.topNItems, appDiskUsageSnapshotData.topNItems);
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end, topNItems);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AppDiskUsageSnapshotData {\n");
    sb.append("    start: ").append(toIndentedString(start)).append("\n");
    sb.append("    end: ").append(toIndentedString(end)).append("\n");
    sb.append("    topNItems: ").append(toIndentedString(topNItems)).append("\n");
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

