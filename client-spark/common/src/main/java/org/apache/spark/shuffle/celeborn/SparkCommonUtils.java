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

package org.apache.spark.shuffle.celeborn;

import java.util.Collections;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.SparkOutOfMemoryError;

import org.apache.celeborn.reflect.DynConstructors;
import org.apache.celeborn.reflect.DynMethods;

public class SparkCommonUtils {
  public static void validateAttemptConfig(SparkConf conf) throws IllegalArgumentException {
    int DEFAULT_MAX_CONSECUTIVE_STAGE_ATTEMPTS = 4;
    int maxStageAttempts =
        conf.getInt("spark.stage.maxConsecutiveAttempts", DEFAULT_MAX_CONSECUTIVE_STAGE_ATTEMPTS);
    // In Spark 2, the parameter is referred to as MAX_TASK_FAILURES, while in Spark 3, it has been
    // changed to TASK_MAX_FAILURES. The default value for both is consistently set to 4.
    int maxTaskAttempts = conf.getInt("spark.task.maxFailures", 4);
    if (maxStageAttempts >= (1 << 15) || maxTaskAttempts >= (1 << 16)) {
      // The map attemptId is a non-negative number constructed from
      // both stageAttemptNumber and taskAttemptNumber.
      // The high 16 bits of the map attemptId are used for the stageAttemptNumber,
      // and the low 16 bits are used for the taskAttemptNumber.
      // So spark.stage.maxConsecutiveAttempts should be less than 32768 (1 << 15)
      // and spark.task.maxFailures should be less than 65536 (1 << 16).
      throw new IllegalArgumentException(
          "The spark.stage.maxConsecutiveAttempts should be less than 32768 (currently "
              + maxStageAttempts
              + ")"
              + "and spark.task.maxFailures should be less than 65536 (currently "
              + maxTaskAttempts
              + ").");
    }
  }

  public static String encodeAppShuffleIdentifier(int appShuffleId, TaskContext context) {
    return appShuffleId + "-" + context.stageId() + "-" + context.stageAttemptNumber();
  }

  public static String[] decodeAppShuffleIdentifier(String appShuffleIdentifier) {
    return appShuffleIdentifier.split("-");
  }

  public static int getEncodedAttemptNumber(TaskContext context) {
    return (context.stageAttemptNumber() << 16) | context.attemptNumber();
  }

  public static void throwSparkOutOfMemoryError() {
    try { // for Spark 3.5 and earlier
      throw DynConstructors.builder()
          .impl(SparkOutOfMemoryError.class, String.class)
          .<SparkOutOfMemoryError>build()
          .newInstance("Not enough memory to grow pointer array");
    } catch (RuntimeException e) {
      // SPARK-44838 (4.0.0)
      DynMethods.StaticMethod isValidErrorClassMethod =
          DynMethods.builder("isValidErrorClass")
              .impl("org.apache.spark.SparkThrowableHelper", String.class)
              .buildStatic();
      // SPARK-49946 (4.0.0) removes single String constructor and introduces
      // _LEGACY_ERROR_TEMP_3301 error condition, SPARK-51386 (4.1.0) renames
      // the error condition to POINTER_ARRAY_OUT_OF_MEMORY.
      if (isValidErrorClassMethod.invoke("POINTER_ARRAY_OUT_OF_MEMORY")) { // for Spark 4.1 onwards
        throw DynConstructors.builder()
            .impl(SparkOutOfMemoryError.class, String.class, Map.class)
            .<SparkOutOfMemoryError>build()
            .newInstance("POINTER_ARRAY_OUT_OF_MEMORY", Collections.EMPTY_MAP);
      } else if (isValidErrorClassMethod.invoke("_LEGACY_ERROR_TEMP_3301")) { // for Spark 4.0
        throw DynConstructors.builder()
            .impl(SparkOutOfMemoryError.class, String.class, Map.class)
            .<SparkOutOfMemoryError>build()
            .newInstance("_LEGACY_ERROR_TEMP_3301", Collections.EMPTY_MAP);
      } else {
        throw new OutOfMemoryError(
            "Unable to construct a SparkOutOfMemoryError, please report this bug to the "
                + "corresponding communities or vendors, and provide the full stack trace.");
      }
    }
  }
}
