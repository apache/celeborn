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

import java.lang.reflect.Field;
import java.util.HashSet;

import org.apache.spark.SparkContext$;
import org.apache.spark.scheduler.DAGScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.spark.RunningStageManager;

public class RunningStageManagerImpl implements RunningStageManager {

  private static final Logger LOG = LoggerFactory.getLogger(RunningStageManagerImpl.class);
  private final Field idField;

  public RunningStageManagerImpl()
      throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
    Class<?> stageClass = Class.forName("org.apache.spark.scheduler.Stage");
    idField = stageClass.getDeclaredField("id");
    idField.setAccessible(true);
  }

  private HashSet<?> runningStages() {
    try {
      DAGScheduler dagScheduler = SparkContext$.MODULE$.getActive().get().dagScheduler();
      Class<?> dagSchedulerClz = SparkContext$.MODULE$.getActive().get().dagScheduler().getClass();
      Field runningStagesField = dagSchedulerClz.getDeclaredField("runningStages");
      return (HashSet<?>) runningStagesField.get(dagScheduler);
    } catch (Exception e) {
      LOG.error("cannot get running stages", e);
      return new HashSet<>();
    }
  }

  public boolean isRunningStage(int stageId) {
    try {
      for (Object stage : runningStages()) {
        int currentStageId = (Integer) idField.get(stage);
        if (currentStageId == stageId) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      LOG.error("unexpected exception when checking whether it is running stage ", e);
      return true;
    }
  }
}
