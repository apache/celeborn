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

package org.apache.celeborn.server.common.service.model;

import java.time.Instant;

public class ClusterTag {
  private Integer id;
  private Integer clusterId;
  private String tag;
  private String workerId;
  private Instant gmtCreate;
  private Instant gmtModify;

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public Integer getClusterId() {
    return clusterId;
  }

  public void setClusterId(Integer clusterId) {
    this.clusterId = clusterId;
  }

  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public String getWorkerId() {
    return workerId;
  }

  public void setWorkerId(String workerId) {
    this.workerId = workerId;
  }

  public Instant getGmtCreate() {
    return gmtCreate;
  }

  public void setGmtCreate(Instant gmtCreate) {
    this.gmtCreate = gmtCreate;
  }

  public Instant getGmtModify() {
    return gmtModify;
  }

  public void setGmtModify(Instant gmtModify) {
    this.gmtModify = gmtModify;
  }
}
