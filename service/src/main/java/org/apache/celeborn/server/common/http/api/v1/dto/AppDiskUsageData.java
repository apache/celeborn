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

import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.celeborn.common.util.Utils;

public class AppDiskUsageData {
  private String appId;
  private Long estimateUsage;

  public AppDiskUsageData(String appId, Long estimateUsage) {
    this.appId = appId;
    this.estimateUsage = estimateUsage;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public Long getEstimateUsage() {
    return estimateUsage;
  }

  public void setEstimateUsage(Long estimateUsage) {
    this.estimateUsage = estimateUsage;
  }

  public String getEstimateUsageStr() {
    return estimateUsage == null ? null : Utils.bytesToString(estimateUsage);
  }

  public void setEstimateUsageStr(String estimateUsageStr) {}

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AppDiskUsageData that = (AppDiskUsageData) o;
    return Objects.equals(getAppId(), that.getAppId())
        && Objects.equals(getEstimateUsage(), that.getEstimateUsage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getAppId(), getEstimateUsage());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
