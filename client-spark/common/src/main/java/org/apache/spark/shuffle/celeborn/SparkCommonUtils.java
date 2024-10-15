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

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;

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

  public static int getEncodedAttemptNumber(TaskContext context) {
    return (context.stageAttemptNumber() << 16) | context.attemptNumber();
  }
}
