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

public class WorkerEventData {
  private WorkerData worker;
  private WorkerEventInfoData event;

  public WorkerEventData(WorkerData worker, WorkerEventInfoData event) {
    this.worker = worker;
    this.event = event;
  }

  public WorkerData getWorker() {
    return worker;
  }

  public void setWorker(WorkerData worker) {
    this.worker = worker;
  }

  public WorkerEventInfoData getEvent() {
    return event;
  }

  public void setEvent(WorkerEventInfoData event) {
    this.event = event;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WorkerEventData that = (WorkerEventData) o;
    return Objects.equals(getWorker(), that.getWorker())
        && Objects.equals(getEvent(), that.getEvent());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getWorker(), getEvent());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
