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

package org.apache.celeborn.common.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allowlist-based deserialization filter to prevent CWE-502 (Deserialization of Untrusted Data)
 * attacks on Celeborn's internal RPC channel.
 *
 * <p>Provides dual-layer defense:
 *
 * <ul>
 *   <li><b>resolveClass allowlist</b> — enforced on all JDK versions via {@link
 *       #createValidatingInputStream}. Blocks any class whose name does not match an allowed
 *       package prefix.
 *   <li><b>JVM-level ObjectInputFilter</b> — enforced on JDK 9+ via {@link #apply}. Adds resource
 *       limits (maxdepth, maxarray, maxrefs, maxbytes) and logs rejected classes. Accessed through
 *       reflection to maintain JDK 8 compatibility; gracefully degrades to no-op on older JDKs.
 * </ul>
 *
 * @see <a href="https://cwe.mitre.org/data/definitions/502.html">CWE-502</a>
 */
public class JavaDeserializerFilter {
  private static final Logger LOG = LoggerFactory.getLogger(JavaDeserializerFilter.class);

  private static final String[] DEFAULT_ALLOWED_PACKAGES = {
    "java.", "scala.", "org.apache.celeborn.", "com.google.protobuf.", "["
  };

  // JDK 9+ ObjectInputFilter API handles, resolved via reflection for JDK 8 compatibility.
  // All fields are null when running on JDK 8.
  private static final Method SET_FILTER_METHOD; // ObjectInputStream.setObjectInputFilter
  private static final Method CREATE_FILTER_METHOD; // ObjectInputFilter.Config.createFilter
  private static final Object STATUS_REJECTED; // ObjectInputFilter.Status.REJECTED
  private static final Object STATUS_ALLOWED; // ObjectInputFilter.Status.ALLOWED
  private static final Method CHECK_METHOD; // ObjectInputFilter.checkInput
  private static final Method SERIAL_CLASS_METHOD; // ObjectInputFilter.FilterInfo.serialClass
  private static final Class<?>[] FILTER_CLASSES; // [ObjectInputFilter.class] for Proxy
  private static final ClassLoader FILTER_CLASS_LOADER;

  // All-or-nothing initialization: if any lookup fails, all handles remain null (JDK 8 path).
  static {
    Method setMethod;
    Method createMethod;
    Object rejected;
    Object allowed;
    Method checkMtd;
    Method serialClassMtd;
    Class<?>[] filterClasses;
    ClassLoader filterCl;
    try {
      Class<?> filterCls = Class.forName("java.io.ObjectInputFilter");
      Class<?> filterInfoCls = Class.forName("java.io.ObjectInputFilter$FilterInfo");
      setMethod = ObjectInputStream.class.getMethod("setObjectInputFilter", filterCls);
      createMethod =
          Class.forName("java.io.ObjectInputFilter$Config").getMethod("createFilter", String.class);
      Class<?> statusCls = Class.forName("java.io.ObjectInputFilter$Status");
      rejected = statusCls.getField("REJECTED").get(null);
      allowed = statusCls.getField("ALLOWED").get(null);
      checkMtd = filterCls.getMethod("checkInput", filterInfoCls);
      serialClassMtd = filterInfoCls.getMethod("serialClass");
      filterClasses = new Class<?>[] {filterCls};
      filterCl = filterCls.getClassLoader();
      if (filterCl == null) {
        filterCl = JavaDeserializerFilter.class.getClassLoader();
      }
    } catch (Exception ignored) {
      // JDK 8: ObjectInputFilter does not exist — force all handles to null.
      setMethod = null;
      createMethod = null;
      rejected = null;
      allowed = null;
      checkMtd = null;
      serialClassMtd = null;
      filterClasses = null;
      filterCl = null;
    }
    SET_FILTER_METHOD = setMethod;
    CREATE_FILTER_METHOD = createMethod;
    STATUS_REJECTED = rejected;
    STATUS_ALLOWED = allowed;
    CHECK_METHOD = checkMtd;
    SERIAL_CLASS_METHOD = serialClassMtd;
    FILTER_CLASSES = filterClasses;
    FILTER_CLASS_LOADER = filterCl;
  }

  private final String[] allowedPackages;
  /**
   * Cached JDK 9+ ObjectInputFilter proxy that delegates to the base filter and logs rejections.
   */
  private final Object loggingFilter;

  private JavaDeserializerFilter(
      String[] allowedPackages,
      int maxDepth,
      int maxArrayLength,
      long maxReferences,
      long maxStreamBytes) {
    if (allowedPackages == null || allowedPackages.length == 0) {
      throw new IllegalArgumentException("allowedPackages must not be null or empty");
    }
    for (String allowedPackage : allowedPackages) {
      if (allowedPackage == null || allowedPackage.isEmpty()) {
        throw new IllegalArgumentException("allowedPackages entry must not be null or empty");
      }
    }
    this.allowedPackages = allowedPackages.clone();
    this.loggingFilter =
        CREATE_FILTER_METHOD != null
            ? createLoggingFilter(
                buildFilterPattern(
                    allowedPackages, maxDepth, maxArrayLength, maxReferences, maxStreamBytes))
            : null;
  }

  /** Wraps the JDK base filter in a Proxy that logs REJECTED classes at WARN level. */
  private Object createLoggingFilter(String filterPattern) {
    try {
      Object baseFilter = CREATE_FILTER_METHOD.invoke(null, filterPattern);
      Object proxy =
          Proxy.newProxyInstance(
              FILTER_CLASS_LOADER,
              FILTER_CLASSES,
              (Object p, Method method, Object[] args) -> {
                Object result = CHECK_METHOD.invoke(baseFilter, args);
                if (STATUS_REJECTED.equals(result)) {
                  try {
                    Class<?> serialClass = (Class<?>) SERIAL_CLASS_METHOD.invoke(args[0]);
                    if (serialClass != null) {
                      // JDK ObjectInputFilter unwraps arrays to their primitive component type,
                      // which then gets rejected by "!*" since primitives match no pattern.
                      // Override REJECTED for arrays whose base component type is primitive
                      // or whose component class name is in the allowlist.
                      if (serialClass.isArray()) {
                        Class<?> component = serialClass;
                        while (component.isArray()) {
                          component = component.getComponentType();
                        }
                        if (component.isPrimitive() || isClassAllowed(component.getName())) {
                          return STATUS_ALLOWED;
                        }
                      }
                      LOG.error("ObjectInputFilter REJECTED class: {}.", serialClass.getName());
                    }
                  } catch (Exception exception) {
                    LOG.error("Error logging rejected class.", exception);
                  }
                }
                return result;
              });
      LOG.debug("Created deserialization filter with pattern: {}.", filterPattern);
      return proxy;
    } catch (Exception exception) {
      LOG.error("Failed to create deserialization filter.", exception);
      return null;
    }
  }

  /**
   * Builds a JDK ObjectInputFilter pattern string. Uses "**" for package prefixes to match
   * subpackages at any depth, and ends with "!*" to reject everything not explicitly allowed. A
   * limit value of 0 means "no limit" per the JDK ObjectInputFilter specification.
   */
  private static String buildFilterPattern(
      String[] allowedPackages,
      int maxDepth,
      int maxArrayLength,
      long maxReferences,
      long maxStreamBytes) {
    StringBuilder pattern = new StringBuilder();
    pattern.append("maxdepth=").append(maxDepth);
    pattern.append(";maxarray=").append(maxArrayLength);
    pattern.append(";maxrefs=").append(maxReferences);
    pattern.append(";maxbytes=").append(maxStreamBytes).append(';');
    for (String allowedPackage : allowedPackages) {
      // Skip "[" — array types are handled by the logging filter proxy which
      // checks component types. The JDK filter unwraps arrays to their primitive
      // component type, making pattern-based matching ineffective for arrays.
      if (allowedPackage.equals("[")) {
        continue;
      }
      pattern.append(allowedPackage).append("**;");
    }
    pattern.append("!*");
    return pattern.toString();
  }

  /** Creates a filter with custom allowed packages and resource limits. */
  public static JavaDeserializerFilter create(
      String[] allowedPackages,
      int maxDepth,
      int maxArrayLength,
      long maxReferences,
      long maxStreamBytes) {
    return new JavaDeserializerFilter(
        allowedPackages, maxDepth, maxArrayLength, maxReferences, maxStreamBytes);
  }

  /** Creates a filter with default allowed packages and no resource limits. */
  public static JavaDeserializerFilter create() {
    return new JavaDeserializerFilter(DEFAULT_ALLOWED_PACKAGES, 0, 0, 0, 0);
  }

  /** Applies the JDK 9+ ObjectInputFilter to the stream. No-op on JDK 8. */
  public void apply(ObjectInputStream inputStream) {
    if (loggingFilter == null) {
      return;
    }
    try {
      SET_FILTER_METHOD.invoke(inputStream, loggingFilter);
    } catch (Exception exception) {
      LOG.error("Failed to apply logging filter.", exception);
    }
  }

  /**
   * Creates an ObjectInputStream that checks each class against the allowlist before resolving.
   *
   * @param classLoader the class loader for resolving classes; null uses the bootstrap loader.
   */
  public ObjectInputStream createValidatingInputStream(
      InputStream inputStream, ClassLoader classLoader) throws IOException {
    return new ObjectInputStream(inputStream) {
      @Override
      protected Class<?> resolveClass(ObjectStreamClass desc)
          throws IOException, ClassNotFoundException {
        String className = desc.getName();
        Class<?> primitive = resolvePrimitiveClass(className);
        if (primitive != null) {
          return primitive;
        }
        if (!isClassAllowed(className)) {
          LOG.error("REJECTED class during deserialization: {}.", className);
          throw new InvalidClassException(className, "Blocked");
        }
        return Class.forName(className, false, classLoader);
      }

      @Override
      protected Class<?> resolveProxyClass(String[] interfaces)
          throws IOException, ClassNotFoundException {
        ClassLoader cl =
            classLoader != null ? classLoader : Thread.currentThread().getContextClassLoader();
        Class<?>[] ifaceClasses = new Class<?>[interfaces.length];
        for (int i = 0; i < interfaces.length; i++) {
          if (!isClassAllowed(interfaces[i])) {
            LOG.error("REJECTED proxy interface during deserialization: {}.", interfaces[i]);
            throw new InvalidClassException(interfaces[i], "Blocked proxy interface");
          }
          ifaceClasses[i] = Class.forName(interfaces[i], false, cl);
        }
        return Proxy.getProxyClass(cl, ifaceClasses);
      }
    };
  }

  protected boolean isClassAllowed(String className) {
    for (String allowedPackage : allowedPackages) {
      if (className.startsWith(allowedPackage)) {
        return true;
      }
    }
    return false;
  }

  private static Class<?> resolvePrimitiveClass(String name) {
    switch (name) {
      case "boolean":
        return boolean.class;
      case "byte":
        return byte.class;
      case "char":
        return char.class;
      case "short":
        return short.class;
      case "int":
        return int.class;
      case "long":
        return long.class;
      case "float":
        return float.class;
      case "double":
        return double.class;
      case "void":
        return void.class;
      default:
        return null;
    }
  }
}
