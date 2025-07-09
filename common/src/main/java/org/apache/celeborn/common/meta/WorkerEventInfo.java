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

package org.apache.celeborn.common.meta;

import java.io.Serializable;
import java.util.Objects;

import org.apache.celeborn.common.protocol.WorkerEventType;

public class WorkerEventInfo implements Serializable {
  private static final long serialVersionUID = 5681914909039445235L;
  private int eventTypeValue;
  private long eventStartTime;

  public WorkerEventInfo(int eventTypeValue, long eventStartTime) {
    this.eventTypeValue = eventTypeValue;
    this.eventStartTime = eventStartTime;
  }

  public boolean isSameEvent(int eventTypeValue) {
    return this.eventTypeValue == eventTypeValue;
  }

  public WorkerEventType getEventType() {
    return WorkerEventType.forNumber(eventTypeValue);
  }

  public long getEventStartTime() {
    return eventStartTime;
  }

  public void setEventStartTime(long eventStartTime) {
    this.eventStartTime = eventStartTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WorkerEventInfo)) {
      return false;
    }
    WorkerEventInfo that = (WorkerEventInfo) o;
    return eventTypeValue == that.eventTypeValue && getEventStartTime() == that.getEventStartTime();
  }

  @Override
  public int hashCode() {
    return Objects.hash(eventTypeValue, getEventStartTime());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WorkerEventInfo{");
    sb.append("eventType=").append(getEventType());
    sb.append(", eventStartTime=").append(eventStartTime);
    sb.append('}');
    return sb.toString();
  }
}
