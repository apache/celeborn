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

package org.apache.celeborn.service.deploy.master.tags

import java.util
import java.util.{Collections, Set => JSet}
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Predicate
import java.util.stream.Collectors

import org.apache.celeborn.common.meta.WorkerInfo

class DefaultTagsFilter(tagsStore: ConcurrentHashMap[String, JSet[String]])
  extends TagsFilter {

  override def filter(tagsExpr: String, workers: util.List[WorkerInfo]): util.List[WorkerInfo] = {
    val tags: Array[String] = tagsExpr.split(",").map(_.trim)
    var workersForTags: Option[JSet[String]] = None

    tags.foreach { tag =>
      val taggedWorkers = tagsStore.getOrDefault(tag, Collections.emptySet())
      workersForTags match {
        case Some(w) =>
          w.retainAll(taggedWorkers)
        case _ =>
          workersForTags = Some(taggedWorkers)
      }
    }

    val workerTagsPredicate = new Predicate[WorkerInfo] {
      override def test(w: WorkerInfo): Boolean = {
        workersForTags.get.contains(w.toUniqueId())
      }
    }

    workers.stream().filter(workerTagsPredicate).collect(Collectors.toList())
  }
}
