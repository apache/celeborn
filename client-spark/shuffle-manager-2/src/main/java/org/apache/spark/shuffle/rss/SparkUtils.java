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

package org.apache.spark.shuffle.rss;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.storage.BlockManagerId;

public class SparkUtils {
  public static MapStatus createMapStatus(
      BlockManagerId loc, long[] uncompressedSizes, long[] uncompressedRecords) throws IOException {

    MapStatus$ status = MapStatus$.MODULE$;
    Class<?> clz = status.getClass();
    Method applyMethod = null;

    for (Method method: clz.getDeclaredMethods()) {
      if ("apply".equals(method.getName())) {
        applyMethod = method;
        break;
      }
    }

    if (applyMethod == null) {
      throw new IOException("Could not find apply method in MapStatus object.");
    }

    try {
      switch (applyMethod.getParameterCount()) {
        case 2:
          // spark 2 without adaptive execution
          return (MapStatus) applyMethod.invoke(status, loc, uncompressedSizes);
        case 3:
          // spark 2 with adaptive execution
          return (MapStatus) applyMethod.invoke(
            status, loc, uncompressedSizes, uncompressedRecords);
        default:
          throw new IllegalStateException(
            "Could not find apply method with correct parameter number in MapStatus object.");
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
