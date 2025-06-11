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
import org.apache.celeborn.rest.v1.model.LoggerInfo;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * LoggerInfos
 */
@JsonPropertyOrder({
  LoggerInfos.JSON_PROPERTY_LOGGERS
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class LoggerInfos {
  public static final String JSON_PROPERTY_LOGGERS = "loggers";
  @javax.annotation.Nullable
  private List<LoggerInfo> loggers = new ArrayList<>();

  public LoggerInfos() {
  }

  public LoggerInfos loggers(@javax.annotation.Nullable List<LoggerInfo> loggers) {
    
    this.loggers = loggers;
    return this;
  }

  public LoggerInfos addLoggersItem(LoggerInfo loggersItem) {
    if (this.loggers == null) {
      this.loggers = new ArrayList<>();
    }
    this.loggers.add(loggersItem);
    return this;
  }

  /**
   * The logger infos.
   * @return loggers
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_LOGGERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<LoggerInfo> getLoggers() {
    return loggers;
  }


  @JsonProperty(JSON_PROPERTY_LOGGERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setLoggers(@javax.annotation.Nullable List<LoggerInfo> loggers) {
    this.loggers = loggers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LoggerInfos loggerInfos = (LoggerInfos) o;
    return Objects.equals(this.loggers, loggerInfos.loggers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(loggers);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class LoggerInfos {\n");
    sb.append("    loggers: ").append(toIndentedString(loggers)).append("\n");
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

