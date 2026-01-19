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

package org.apache.celeborn.plugin.flink.utils;

import java.lang.reflect.Field;

/** Utility methods for reflection. */
public class ReflectionUtils {

  @SuppressWarnings("unchecked")
  public static <T> T readDeclaredFieldRecursiveNoThrow(Object object, String fieldName) {
    try {
      Field field = findFieldRecursive(object.getClass(), fieldName);
      field.setAccessible(true);
      return (T) field.get(object);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static Field findFieldRecursive(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    Class<?> cur = clazz;
    while (cur != null) {
      try {
        return cur.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
        cur = cur.getSuperclass();
      }
    }
    throw new NoSuchFieldException(
        "Field '" + fieldName + "' not found in class hierarchy of " + clazz.getSimpleName());
  }

  @SuppressWarnings("unchecked")
  public static <T> T createEmptyImplementation(Class<T> interfaceClass) {
    if (!interfaceClass.isInterface()) {
      throw new IllegalArgumentException("Class must be an interface: " + interfaceClass.getName());
    }

    return (T)
        java.lang.reflect.Proxy.newProxyInstance(
            interfaceClass.getClassLoader(),
            new Class<?>[] {interfaceClass},
            (proxy, method, args) -> {
              // Return default value based on return type
              if (method.getReturnType() == boolean.class) {
                return false;
              } else if (method.getReturnType() == byte.class) {
                return (byte) 0;
              } else if (method.getReturnType() == char.class) {
                return (char) 0;
              } else if (method.getReturnType() == short.class) {
                return (short) 0;
              } else if (method.getReturnType() == int.class) {
                return 0;
              } else if (method.getReturnType() == long.class) {
                return 0L;
              } else if (method.getReturnType() == float.class) {
                return 0.0f;
              } else if (method.getReturnType() == double.class) {
                return 0.0d;
              } else {
                // For object types, return null
                return null;
              }
            });
  }
}
