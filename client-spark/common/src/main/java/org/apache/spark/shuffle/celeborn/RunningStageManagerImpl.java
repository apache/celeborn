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

import java.util.HashSet;

import scala.collection.JavaConverters;

import org.apache.spark.SparkContext$;
import org.apache.spark.scheduler.DAGScheduler;
import org.apache.spark.scheduler.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.reflect.DynFields;
import org.apache.celeborn.spark.RunningStageManager;

public class RunningStageManagerImpl implements RunningStageManager {

  private static final Logger LOG = LoggerFactory.getLogger(RunningStageManagerImpl.class);
  private static final DynFields.UnboundField id_FIELD =
      DynFields.builder().hiddenImpl(Stage.class, "id").build();
  private static final DynFields.UnboundField runningStages_FIELD =
      DynFields.builder().hiddenImpl(DAGScheduler.class, "runningStages").build();

  private HashSet<?> runningStages() {
    try {
      DAGScheduler dagScheduler = SparkContext$.MODULE$.getActive().get().dagScheduler();
      return new HashSet<>(
          JavaConverters.asJavaCollectionConverter(
                  (scala.collection.mutable.HashSet<?>)
                      runningStages_FIELD.bind(dagScheduler).get())
              .asJavaCollection());
    } catch (Exception e) {
      LOG.error("cannot get running stages", e);
      return new HashSet<>();
    }
  }

  public boolean isRunningStage(int stageId) {
    try {
      for (Object stage : runningStages()) {
        int currentStageId = (Integer) id_FIELD.bind(stage).get();
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
