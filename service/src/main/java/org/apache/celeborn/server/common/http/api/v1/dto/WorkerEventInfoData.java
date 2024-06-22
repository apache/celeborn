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

public class WorkerEventInfoData {
  private String eventType;
  private Long eventStartTime;

  public WorkerEventInfoData(String eventType, Long eventStartTime) {
    this.eventType = eventType;
    this.eventStartTime = eventStartTime;
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public Long getEventStartTime() {
    return eventStartTime;
  }

  public void setEventStartTime(Long eventStartTime) {
    this.eventStartTime = eventStartTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WorkerEventInfoData that = (WorkerEventInfoData) o;
    return Objects.equals(getEventType(), that.getEventType())
        && Objects.equals(getEventStartTime(), that.getEventStartTime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getEventType(), getEventStartTime());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
