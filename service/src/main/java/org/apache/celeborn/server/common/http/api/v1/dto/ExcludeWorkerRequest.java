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

import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ExcludeWorkerRequest {
  private List<WorkerId> add;
  private List<WorkerId> remove;

  public ExcludeWorkerRequest(List<WorkerId> add, List<WorkerId> remove) {
    this.add = add;
    this.remove = remove;
  }

  public List<WorkerId> getAdd() {
    return add;
  }

  public void setAdd(List<WorkerId> add) {
    this.add = add;
  }

  public List<WorkerId> getRemove() {
    return remove;
  }

  public void setRemove(List<WorkerId> remove) {
    this.remove = remove;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExcludeWorkerRequest that = (ExcludeWorkerRequest) o;
    return Objects.equals(getAdd(), that.getAdd()) && Objects.equals(getRemove(), that.getRemove());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getAdd(), getRemove());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
