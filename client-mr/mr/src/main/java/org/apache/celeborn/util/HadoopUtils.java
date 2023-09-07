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

package org.apache.celeborn.util;

import java.util.Map;

import org.apache.hadoop.mapred.JobConf;

import org.apache.celeborn.common.CelebornConf;

public class HadoopUtils {
  public static final String MR_CELEBORN_CONF = "celeborn.xml";
  public static final String MR_CELEBORN_LM_HOST = "celeborn.lifecycleManager.host";
  public static final String MR_CELEBORN_LM_PORT = "celeborn.lifecycleManager.port";
  public static final String MR_CELEBORN_APPLICATION_ID = "celeborn.applicationId";

  public static CelebornConf fromYarnConf(JobConf conf) {
    CelebornConf tmpCelebornConf = new CelebornConf();
    for (Map.Entry<String, String> property : conf) {
      String proName = property.getKey();
      String proValue = property.getValue();
      if (proName.startsWith("mapreduce.celeborn")) {
        tmpCelebornConf.set(proName.substring("mapreduce.".length()), proValue);
      }
    }
    return tmpCelebornConf;
  }
}
