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

import java.util.Objects;

import org.apache.celeborn.common.protocol.PbWorkerStatus;

public class WorkerStatus {
  private int stateValue;
  private long stateStartTime;

  public WorkerStatus(int stateValue, long stateStartTime) {
    this.stateValue = stateValue;
    this.stateStartTime = stateStartTime;
  }

  public int getStateValue() {
    return stateValue;
  }

  public PbWorkerStatus.State getState() {
    return PbWorkerStatus.State.forNumber(stateValue);
  }

  public long getStateStartTime() {
    return stateStartTime;
  }

  public static WorkerStatus normalWorkerStatus() {
    return new WorkerStatus(PbWorkerStatus.State.Normal.getNumber(), System.currentTimeMillis());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WorkerStatus)) {
      return false;
    }
    WorkerStatus that = (WorkerStatus) o;
    return getStateValue() == that.getStateValue()
        && getStateStartTime() == that.getStateStartTime();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStateValue(), getStateStartTime());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WorkerStatus{");
    sb.append("state=").append(getState());
    sb.append(", stateStartTime=").append(stateStartTime);
    sb.append('}');
    return sb.toString();
  }
}
