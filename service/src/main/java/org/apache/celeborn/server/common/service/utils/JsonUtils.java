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

package org.apache.celeborn.server.common.service.utils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

public class JsonUtils {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
  }

  public static String toJson(Object obj) {
    try {
      return MAPPER.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T fromJson(String json, Class<T> clazz) throws JsonProcessingException{
    try {
      if (StringUtils.isEmpty(json)) {
        return null;
      }
      // Create secure ObjectMapper instance with deactivated default typing
      return new ObjectMapper()
              .setAnnotationIntrospector(new JacksonAnnotationIntrospector())
              .deactivateDefaultTyping()  // CRITICAL: Prevents polymorphic deserialization attacks
              .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
              .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
              .readValue(json, clazz);
    } catch (JsonParseException e) {
      String errMsg = "The input JSON could not be parsed";
      throw new RuntimeException(errMsg, e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
