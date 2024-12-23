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

import java.util
import java.util.{Set => JSet}
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Predicate
import java.util.stream.Collectors

import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.service.deploy.master.tags.TagsFilter

class TagsQLFilter(tagsStore: ConcurrentHashMap[String, JSet[String]])
  extends TagsFilter {

  private val TAGS_KV_SEPARATOR = "="

  override def filter(tagsExpr: String, workers: util.List[WorkerInfo]): util.List[WorkerInfo] = {
    val parser = new TagsQLParser()
    val nodes = parser.parse(tagsExpr)

    var positiveWorkerSet: Option[util.HashSet[String]] = None
    val negativeWorkerSet = new util.HashSet[String]

    nodes.foreach { node =>
      val tagKey = node.key

      val workerForNode = new util.HashSet[String]
      node.values.foreach { tagValue =>
        val tag = s"$tagKey$TAGS_KV_SEPARATOR$tagValue"
        val taggedWorkers = tagsStore.getOrDefault(tag, util.Collections.emptySet())
        workerForNode.addAll(taggedWorkers)
      }

      node.operator match {
        case Equals =>
          positiveWorkerSet match {
            case Some(w) =>
              w.retainAll(workerForNode)
            case _ =>
              positiveWorkerSet = Some(workerForNode)
          }
        case NotEquals =>
          negativeWorkerSet.addAll(workerForNode)
      }
    }

    val workerTagsPredicate = new Predicate[WorkerInfo] {
      override def test(w: WorkerInfo): Boolean = {
        positiveWorkerSet.get.contains(w.toUniqueId()) && !negativeWorkerSet.contains(
          w.toUniqueId())
      }
    }

    workers.stream().filter(workerTagsPredicate).collect(Collectors.toList())
  }
}
