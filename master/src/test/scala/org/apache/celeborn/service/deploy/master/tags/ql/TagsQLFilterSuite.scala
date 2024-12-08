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

package org.apache.celeborn.service.deploy.master.tags.ql

import java.util.{Set => JSet}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter, setAsJavaSetConverter}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.meta.WorkerInfo

class TagsQLFilterSuite extends CelebornFunSuite {

  private val WORKER1 = new WorkerInfo("host1", 111, 112, 113, 114, 115)
  private val WORKER2 = new WorkerInfo("host2", 211, 212, 213, 214, 215)
  private val WORKER3 = new WorkerInfo("host3", 311, 312, 313, 314, 315)
  private val workers = List(WORKER1, WORKER2, WORKER3).asJava

  private val tagsMap = new ConcurrentHashMap[String, JSet[String]]()
  tagsMap.putAll(Map(
    "env=production" -> Set(WORKER1.toUniqueId(), WORKER2.toUniqueId()).asJava,
    "env=staging" -> Set(WORKER3.toUniqueId()).asJava,
    "region=us-east" -> Set(WORKER1.toUniqueId(), WORKER3.toUniqueId()).asJava,
    "region=us-west" -> Set(WORKER2.toUniqueId(), WORKER3.toUniqueId()).asJava).asJava)

  test("Test TagsQLFilter") {
    val tagsFilter = new TagsQLFilter(tagsMap)

    {
      val tagsExpr = "env:production region:{us-east,us-west}"
      val taggedWorkers = tagsFilter.filter(tagsExpr, workers)
      assert(taggedWorkers.size() == 2)
      assert(taggedWorkers.contains(WORKER1))
      assert(taggedWorkers.contains(WORKER2))
    }

    {
      val tagsExpr = "env:production region:!{us-east,us-west}"
      val taggedWorkers = tagsFilter.filter(tagsExpr, workers)
      assert(taggedWorkers.size() == 0)
    }

    {
      val tagsExpr = "env:staging region:{us-east,us-west}"
      val taggedWorkers = tagsFilter.filter(tagsExpr, workers)
      assert(taggedWorkers.size() == 1)
      assert(taggedWorkers.contains(WORKER3))
    }

    {
      val tagsExpr = "env:!staging region:{us-east,us-west}"
      val taggedWorkers = tagsFilter.filter(tagsExpr, workers)
      assert(taggedWorkers.size() == 2)
      assert(taggedWorkers.contains(WORKER1))
      assert(taggedWorkers.contains(WORKER2))
    }
  }
}
