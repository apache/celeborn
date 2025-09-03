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

package org.apache.celeborn.service.deploy.master.clustermeta.ha

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.AbstractSource

import java.util.concurrent.ConcurrentHashMap

class MetaSource(conf: CelebornConf) extends AbstractSource(conf, MetaSource.ROLE_META) {
  override def sourceName: String = MetaSource.ROLE_META

  private val msgNameSet = ConcurrentHashMap.newKeySet[String]()

  override def updateTimer(name: String, value: Long): Unit = {
    if (!msgNameSet.contains(name)) {
      addTimer(name)
      msgNameSet.add(name)
    }
    super.updateTimer(name, value)
  }

  addTimer(MetaSource.PROCESS_TIME)
  msgNameSet.add(MetaSource.PROCESS_TIME)
  startCleaner()
}

object MetaSource {
  val ROLE_META = "META"

  val PROCESS_TIME = "ProcessTime"
}