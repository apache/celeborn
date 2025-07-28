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

import scala.collection.JavaConverters.{asScalaIteratorConverter, mapAsScalaConcurrentMapConverter}

import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.util.JavaUtils
import org.apache.celeborn.server.common.service.config.ConfigService

class TagsManager(configService: Option[ConfigService]) extends Logging {
  private val defaultTagStore = JavaUtils.newConcurrentHashMap[String, JSet[String]]()

  private val addNewTagFunc =
    new util.function.Function[String, ConcurrentHashMap.KeySetView[String, java.lang.Boolean]]() {
      override def apply(t: String): ConcurrentHashMap.KeySetView[String, java.lang.Boolean] =
        ConcurrentHashMap.newKeySet[String]()
    }

  def getTagStore: ConcurrentHashMap[String, JSet[String]] = {
    configService match {
      case Some(cs) =>
        // TODO: Make configStore.getTags return ConcurrentMap
        JavaUtils.newConcurrentHashMap(cs.getSystemConfigFromCache.getTags)
      case _ =>
        defaultTagStore
    }
  }

  def getTaggedWorkers(
      userIdentifier: UserIdentifier,
      clientTagsExpr: String,
      workers: util.List[WorkerInfo]): util.List[WorkerInfo] = {

    val tagsExpr = configService.flatMap { cs =>
      val config = cs.getTenantUserConfigFromCache(userIdentifier.tenantId, userIdentifier.name)
      val tagsMeta = config.getWorkerTagsMeta
      if (tagsMeta.preferClientTagExpr) {
        Some(clientTagsExpr)
      } else {
        Some(tagsMeta.tagsExpr)
      }
    }.getOrElse(clientTagsExpr)

    if (tagsExpr.isEmpty) {
      logWarning("No tags provided")
      return workers
    }

    val tags = tagsExpr.split(",").map(_.trim)

    var workersForTags: Option[JSet[String]] = None
    tags.foreach { tag =>
      val taggedWorkers = getTagStore.getOrDefault(tag, Collections.emptySet())
      workersForTags match {
        case Some(w) =>
          w.retainAll(taggedWorkers)
        case _ =>
          workersForTags = Some(taggedWorkers)
      }
    }

    if (workersForTags.isEmpty) {
      logWarning(s"No workers for tags: $tagsExpr found in cluster")
      return Collections.emptyList()
    }

    val workerTagsPredicate = new Predicate[WorkerInfo] {
      override def test(w: WorkerInfo): Boolean = workersForTags.get.contains(w.toUniqueId)
    }
    workers.stream().filter(workerTagsPredicate).collect(Collectors.toList())
  }

  def addTagToWorker(tag: String, workerId: String): Unit = {
    val workers = defaultTagStore.computeIfAbsent(tag, addNewTagFunc)
    logInfo(s"Adding Tag $tag to worker $workerId")
    workers.add(workerId)
  }

  def removeTagFromWorker(tag: String, workerId: String): Unit = {
    val workers = defaultTagStore.get(tag)

    if (workers != null && workers.contains(workerId)) {
      logInfo(s"Removing Tag $tag from worker $workerId")
      workers.remove(workerId)
    } else {
      logWarning(s"Tag $tag not found for worker $workerId")
    }
  }

  def getTagsForWorker(worker: WorkerInfo): Set[String] = {
    defaultTagStore.asScala.filter(_._2.contains(worker.toUniqueId)).keySet.toSet
  }

  def removeTagFromCluster(tag: String): Unit = {
    val workers = defaultTagStore.remove(tag)
    if (workers != null) {
      logInfo(s"Removed Tag $tag from cluster with workers ${workers.toArray.mkString(", ")}")
    } else {
      logWarning(s"Tag $tag not found in cluster and thus can not be removed")
    }
  }

  def getTagsForCluster: Set[String] = {
    defaultTagStore.keySet().iterator().asScala.toSet
  }
}
