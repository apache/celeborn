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


package org.apache.celeborn.rest.v1.model;

import java.util.Objects;
import java.util.Arrays;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * DeleteDynamicConfigRequest
 */
@JsonPropertyOrder({
  DeleteDynamicConfigRequest.JSON_PROPERTY_LEVEL,
  DeleteDynamicConfigRequest.JSON_PROPERTY_CONFIGS,
  DeleteDynamicConfigRequest.JSON_PROPERTY_TENANT,
  DeleteDynamicConfigRequest.JSON_PROPERTY_NAME
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.8.0")
public class DeleteDynamicConfigRequest {
  /**
   * The config level of dynamic configs.
   */
  public enum LevelEnum {
    SYSTEM("SYSTEM"),
    
    TENANT("TENANT"),
    
    TENANT_USER("TENANT_USER");

    private String value;

    LevelEnum(String value) {
      this.value = value;
    }

    @JsonValue
    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static LevelEnum fromValue(String value) {
      for (LevelEnum b : LevelEnum.values()) {
        if (b.value.equalsIgnoreCase(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unexpected value '" + value + "'");
    }
  }

  public static final String JSON_PROPERTY_LEVEL = "level";
  private LevelEnum level;

  public static final String JSON_PROPERTY_CONFIGS = "configs";
  private List<String> configs = new ArrayList<>();

  public static final String JSON_PROPERTY_TENANT = "tenant";
  private String tenant;

  public static final String JSON_PROPERTY_NAME = "name";
  private String name;

  public DeleteDynamicConfigRequest() {
  }

  public DeleteDynamicConfigRequest level(LevelEnum level) {
    
    this.level = level;
    return this;
  }

  /**
   * The config level of dynamic configs.
   * @return level
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_LEVEL)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public LevelEnum getLevel() {
    return level;
  }


  @JsonProperty(JSON_PROPERTY_LEVEL)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setLevel(LevelEnum level) {
    this.level = level;
  }

  public DeleteDynamicConfigRequest configs(List<String> configs) {
    
    this.configs = configs;
    return this;
  }

  public DeleteDynamicConfigRequest addConfigsItem(String configsItem) {
    if (this.configs == null) {
      this.configs = new ArrayList<>();
    }
    this.configs.add(configsItem);
    return this;
  }

  /**
   * The dynamic configs to delete.
   * @return configs
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_CONFIGS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public List<String> getConfigs() {
    return configs;
  }


  @JsonProperty(JSON_PROPERTY_CONFIGS)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setConfigs(List<String> configs) {
    this.configs = configs;
  }

  public DeleteDynamicConfigRequest tenant(String tenant) {
    
    this.tenant = tenant;
    return this;
  }

  /**
   * The tenant id of TENANT or TENANT_USER level.
   * @return tenant
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_TENANT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getTenant() {
    return tenant;
  }


  @JsonProperty(JSON_PROPERTY_TENANT)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public DeleteDynamicConfigRequest name(String name) {
    
    this.name = name;
    return this;
  }

  /**
   * The user name of TENANT_USER level.
   * @return name
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getName() {
    return name;
  }


  @JsonProperty(JSON_PROPERTY_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeleteDynamicConfigRequest deleteDynamicConfigRequest = (DeleteDynamicConfigRequest) o;
    return Objects.equals(this.level, deleteDynamicConfigRequest.level) &&
        Objects.equals(this.configs, deleteDynamicConfigRequest.configs) &&
        Objects.equals(this.tenant, deleteDynamicConfigRequest.tenant) &&
        Objects.equals(this.name, deleteDynamicConfigRequest.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(level, configs, tenant, name);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DeleteDynamicConfigRequest {\n");
    sb.append("    level: ").append(toIndentedString(level)).append("\n");
    sb.append("    configs: ").append(toIndentedString(configs)).append("\n");
    sb.append("    tenant: ").append(toIndentedString(tenant)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}

