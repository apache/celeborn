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

package org.apache.celeborn.server.common.http.api.v1.dto;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class AppDiskUsageSnapshotData {
  private Long start;
  private Long end;
  private List<AppDiskUsageData> topNItems;

  public AppDiskUsageSnapshotData(Long start, Long end, List<AppDiskUsageData> topNItems) {
    this.start = start;
    this.end = end;
    this.topNItems = topNItems;
  }

  public Long getStart() {
    return start;
  }

  public void setStart(Long start) {
    this.start = start;
  }

  public Long getEnd() {
    return end;
  }

  public void setEnd(Long end) {
    this.end = end;
  }

  public List<AppDiskUsageData> getTopNItems() {
    if (topNItems == null) {
      return Collections.EMPTY_LIST;
    }
    return topNItems;
  }

  public void setTopNItems(List<AppDiskUsageData> topNItems) {
    this.topNItems = topNItems;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AppDiskUsageSnapshotData that = (AppDiskUsageSnapshotData) o;
    return Objects.equals(getStart(), that.getStart())
        && Objects.equals(getEnd(), that.getEnd())
        && Objects.equals(getTopNItems(), that.getTopNItems());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStart(), getEnd(), getTopNItems());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
