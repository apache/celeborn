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

package org.apache.celeborn.service.deploy.master.util

import org.apache.hadoop.conf.Configuration

import org.apache.celeborn.common.CelebornConf

object CelebornHadoopUtils {
  private[celeborn] def newConfiguration(conf: CelebornConf): Configuration = {
    val hadoopConf = new Configuration()
    appendSparkHadoopConfigs(conf, hadoopConf)
    hadoopConf
  }

  private def appendSparkHadoopConfigs(conf: CelebornConf, hadoopConf: Configuration): Unit = {
    // Copy any "celeborn.hadoop.foo=bar" celeborn properties into conf as "foo=bar"
    for ((key, value) <- conf.getAll if key.startsWith("celeborn.hadoop.")) {
      hadoopConf.set(key.substring("celeborn.hadoop.".length), value)
    }
  }
}
