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
import java.util.function.Predicate
import java.util.stream.Collectors

import scala.collection.JavaConverters._

import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.server.common.service.config.ConfigService

class TagsManager(configService: Option[ConfigService]) extends Logging {

  private def tagStore: Option[util.Map[String, JSet[String]]] = {
    configService match {
      case Some(cs) => Option(cs.getSystemConfigFromCache.getTags)
      case _ => None
    }
  }

  private def resolveTagsExpr(userIdentifier: UserIdentifier, clientTagsExpr: String): String =
    configService.map { cs =>
      val tagsMeta = cs
        .getTenantUserConfigFromCache(userIdentifier.tenantId, userIdentifier.name)
        .getWorkerTagsMeta
      if (tagsMeta.preferClientTagExpr) clientTagsExpr else tagsMeta.tagsExpr
    }.getOrElse(clientTagsExpr)

  def getTaggedWorkers(
      userIdentifier: UserIdentifier,
      clientTagsExpr: String,
      workers: util.List[WorkerInfo]): util.List[WorkerInfo] = {

    val tags = resolveTagsExpr(userIdentifier, clientTagsExpr)
      .split(",").map(_.trim).filter(_.nonEmpty)

    if (tags.isEmpty) {
      logDebug("No tags provided, returning all workers")
      return workers
    }

    val workerTagsPredicate = new Predicate[WorkerInfo] {
      override def test(w: WorkerInfo): Boolean = tags.forall { tag =>
        w.tags.contains(tag) ||
        tagStore.exists(_.getOrDefault(tag, Collections.emptySet()).contains(w.toUniqueId))
      }
    }
    workers.stream().filter(workerTagsPredicate).collect(Collectors.toList())
  }

  def getTagsForWorker(worker: WorkerInfo): Set[String] = {
    val storeTags = tagStore.map(_.asScala.collect {
      case (tag, workerIds) if workerIds.contains(worker.toUniqueId) => tag
    }.toSet).getOrElse(Set.empty)
    storeTags ++ worker.tags.asScala
  }

  def getTagsForCluster: Set[String] = {
    tagStore.map(_.keySet.asScala.toSet).getOrElse(Set.empty)
  }
}
