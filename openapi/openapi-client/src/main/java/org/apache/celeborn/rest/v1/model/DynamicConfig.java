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
import org.apache.celeborn.rest.v1.model.ConfigData;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * DynamicConfig
 */
@JsonPropertyOrder({
  DynamicConfig.JSON_PROPERTY_LEVEL,
  DynamicConfig.JSON_PROPERTY_DESC,
  DynamicConfig.JSON_PROPERTY_CONFIGS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class DynamicConfig {
  /**
   * the config level of dynamic configs.
   */
  public enum LevelEnum {
    SYSTEM(String.valueOf("SYSTEM")),
    
    TENANT(String.valueOf("TENANT")),
    
    TENANT_USER(String.valueOf("TENANT_USER"));

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
  @javax.annotation.Nullable
  private LevelEnum level;

  public static final String JSON_PROPERTY_DESC = "desc";
  @javax.annotation.Nullable
  private String desc;

  public static final String JSON_PROPERTY_CONFIGS = "configs";
  @javax.annotation.Nullable
  private List<ConfigData> configs = new ArrayList<>();

  public DynamicConfig() {
  }

  public DynamicConfig level(@javax.annotation.Nullable LevelEnum level) {
    
    this.level = level;
    return this;
  }

  /**
   * the config level of dynamic configs.
   * @return level
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_LEVEL)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public LevelEnum getLevel() {
    return level;
  }


  @JsonProperty(JSON_PROPERTY_LEVEL)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setLevel(@javax.annotation.Nullable LevelEnum level) {
    this.level = level;
  }

  public DynamicConfig desc(@javax.annotation.Nullable String desc) {
    
    this.desc = desc;
    return this;
  }

  /**
   * additional description of the dynamic config, such as tenantId and user identifier.
   * @return desc
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_DESC)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getDesc() {
    return desc;
  }


  @JsonProperty(JSON_PROPERTY_DESC)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setDesc(@javax.annotation.Nullable String desc) {
    this.desc = desc;
  }

  public DynamicConfig configs(@javax.annotation.Nullable List<ConfigData> configs) {
    
    this.configs = configs;
    return this;
  }

  public DynamicConfig addConfigsItem(ConfigData configsItem) {
    if (this.configs == null) {
      this.configs = new ArrayList<>();
    }
    this.configs.add(configsItem);
    return this;
  }

  /**
   * the config items.
   * @return configs
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CONFIGS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<ConfigData> getConfigs() {
    return configs;
  }


  @JsonProperty(JSON_PROPERTY_CONFIGS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setConfigs(@javax.annotation.Nullable List<ConfigData> configs) {
    this.configs = configs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DynamicConfig dynamicConfig = (DynamicConfig) o;
    return Objects.equals(this.level, dynamicConfig.level) &&
        Objects.equals(this.desc, dynamicConfig.desc) &&
        Objects.equals(this.configs, dynamicConfig.configs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(level, desc, configs);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class DynamicConfig {\n");
    sb.append("    level: ").append(toIndentedString(level)).append("\n");
    sb.append("    desc: ").append(toIndentedString(desc)).append("\n");
    sb.append("    configs: ").append(toIndentedString(configs)).append("\n");
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

