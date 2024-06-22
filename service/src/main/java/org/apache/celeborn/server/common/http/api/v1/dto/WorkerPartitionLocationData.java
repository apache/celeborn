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
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class WorkerPartitionLocationData {
  private Map<String, Map<String, PartitionLocationData>> primaryPartitions;
  private Map<String, Map<String, PartitionLocationData>> replicaPartitions;

  public WorkerPartitionLocationData(
      Map<String, Map<String, PartitionLocationData>> primaryPartitions,
      Map<String, Map<String, PartitionLocationData>> replicaPartitions) {
    this.primaryPartitions = primaryPartitions;
    this.replicaPartitions = replicaPartitions;
  }

  public Map<String, Map<String, PartitionLocationData>> getPrimaryPartitions() {
    if (primaryPartitions == null) {
      return Collections.EMPTY_MAP;
    }
    return primaryPartitions;
  }

  public void setPrimaryPartitions(
      Map<String, Map<String, PartitionLocationData>> primaryPartitions) {
    this.primaryPartitions = primaryPartitions;
  }

  public Map<String, Map<String, PartitionLocationData>> getReplicaPartitions() {
    if (replicaPartitions == null) {
      return Collections.EMPTY_MAP;
    }
    return replicaPartitions;
  }

  public void setReplicaPartitions(
      Map<String, Map<String, PartitionLocationData>> replicaPartitions) {
    this.replicaPartitions = replicaPartitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WorkerPartitionLocationData that = (WorkerPartitionLocationData) o;
    return Objects.equals(getPrimaryPartitions(), that.getPrimaryPartitions())
        && Objects.equals(getReplicaPartitions(), that.getReplicaPartitions());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPrimaryPartitions(), getReplicaPartitions());
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
  }
}
