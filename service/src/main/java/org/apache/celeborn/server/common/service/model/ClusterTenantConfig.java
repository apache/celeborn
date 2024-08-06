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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public class ClusterTenantConfig {

  private Integer id;
  private Integer clusterId;
  private String tenantId;
  private String level;
  private String name;
  private String configKey;
  private String configValue;
  private String type;
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

  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  public String getName() {
    return StringUtils.isBlank(name) ? null : name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getConfigKey() {
    return configKey;
  }

  public void setConfigKey(String configKey) {
    this.configKey = configKey;
  }

  public String getConfigValue() {
    return configValue;
  }

  public void setConfigValue(String configValue) {
    this.configValue = configValue;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
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

  public Pair getTenantInfo() {
    return Pair.of(tenantId, name);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ClusterTenantConfig{");
    sb.append("id=").append(id);
    sb.append(", clusterId=").append(clusterId);
    sb.append(", tenantId='").append(tenantId).append('\'');
    sb.append(", level='").append(level).append('\'');
    sb.append(", user='").append(name).append('\'');
    sb.append(", configKey='").append(configKey).append('\'');
    sb.append(", configValue='").append(configValue).append('\'');
    sb.append(", type='").append(type).append('\'');
    sb.append(", gmtCreate=").append(gmtCreate);
    sb.append(", gmtModify=").append(gmtModify);
    sb.append('}');
    return sb.toString();
  }
}
