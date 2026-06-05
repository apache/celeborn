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

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.server.common.service.config.{ConfigService, DynamicConfigServiceFactory}

class TagsManagerSuite extends CelebornFunSuite {
  private var tagsManager: TagsManager = _

  private val TAG1 = "tag1"
  private val TAG2 = "tag2"

  private val WORKER1 = new WorkerInfo("host1", 111, 112, 113, 114, 115)
  private val WORKER2 = new WorkerInfo("host2", 211, 212, 213, 214, 215)
  private val WORKER3 = new WorkerInfo("host3", 311, 312, 313, 314, 315)

  private val workers = List(WORKER1, WORKER2, WORKER3).asJava

  private val user = UserIdentifier("tenant_01", "Jerry")
  private var configService: ConfigService = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    DynamicConfigServiceFactory.reset()

    val conf = new CelebornConf()
    conf.set(CelebornConf.DYNAMIC_CONFIG_STORE_BACKEND, "FS")
    conf.set(
      CelebornConf.DYNAMIC_CONFIG_STORE_FS_PATH.key,
      getTestResourceFile("dynamicConfig-tags.yaml").getPath)
    configService = DynamicConfigServiceFactory.getConfigService(conf)
  }

  private def workerWithTags(host: String, tags: String*): WorkerInfo = {
    val w = new WorkerInfo(host, 111, 112, 113, 114, 115)
    w.tags = new java.util.HashSet[String](tags.asJava)
    w
  }

  test("getTaggedWorkers filters by worker self-registered tags (no config service)") {
    tagsManager = new TagsManager(None)

    val w1 = workerWithTags("host1", TAG1)
    val w2 = workerWithTags("host2", TAG1, TAG2)
    val w3 = workerWithTags("host3", TAG2)
    val workers = List(w1, w2, w3).asJava

    {
      val tagged = tagsManager.getTaggedWorkers(user, TAG1, workers)
      assert(tagged.size == 2)
      assert(tagged.contains(w1) && tagged.contains(w2) && !tagged.contains(w3))
    }
    {
      val tagged = tagsManager.getTaggedWorkers(user, "tag1,tag2", workers)
      assert(tagged.size == 1 && tagged.contains(w2))
    }
    {
      val tagged = tagsManager.getTaggedWorkers(user, "tag1,tag3", workers)
      assert(tagged.isEmpty)
    }
    {
      assert(tagsManager.getTaggedWorkers(user, "unknown-tag", workers).isEmpty)
    }
    {
      assert(tagsManager.getTagsForWorker(w2) == Set(TAG1, TAG2))
      assert(tagsManager.getTagsForWorker(workerWithTags("host4")).isEmpty)
      assert(tagsManager.getTagsForCluster.isEmpty)
    }
  }

  test("test tags manager with config service") {
    tagsManager = new TagsManager(Option(configService))

    {
      // preferClientTagsExpr: true
      val taggedWorkers = tagsManager.getTaggedWorkers(user, TAG1, workers)
      assert(taggedWorkers.size == 2)
      assert(taggedWorkers.contains(WORKER1))
      assert(taggedWorkers.contains(WORKER2))
      assert(!taggedWorkers.contains(WORKER3))
    }

    {
      // preferClientTagsExpr: true
      val taggedWorkers = tagsManager.getTaggedWorkers(user, TAG2, workers)
      assert(taggedWorkers.size == 2)
      assert(!taggedWorkers.contains(WORKER1))
      assert(taggedWorkers.contains(WORKER2))
      assert(taggedWorkers.contains(WORKER3))
    }

    {
      // preferClientTagsExpr: false, adminTagsExpr: "tag1"
      val user = UserIdentifier("tenant_01", "Tom")
      val taggedWorkers = tagsManager.getTaggedWorkers(user, "tag1,tag2", workers)
      assert(taggedWorkers.size == 2)
      assert(taggedWorkers.contains(WORKER1))
      assert(taggedWorkers.contains(WORKER2))
      assert(!taggedWorkers.contains(WORKER3))
    }

    {
      // preferClientTagsExpr: false, adminTagsExpr: ""
      val user = UserIdentifier("tenant_01", "Robin")
      val taggedWorkers = tagsManager.getTaggedWorkers(user, "tag1", workers)
      assert(taggedWorkers.size == 3)
      assert(taggedWorkers.contains(WORKER1))
      assert(taggedWorkers.contains(WORKER2))
      assert(taggedWorkers.contains(WORKER3))
    }
  }

  test("getTaggedWorkers matches workers tagged via either config store or self-registration") {
    tagsManager = new TagsManager(Option(configService))
    val selfTaggedWorker = workerWithTags("host4", TAG1)
    val all = List(WORKER1, WORKER2, WORKER3, selfTaggedWorker).asJava

    val tagged = tagsManager.getTaggedWorkers(user, TAG1, all)
    assert(tagged.size == 3) // WORKER1, WORKER2 (config store) + selfTagged (self)
    assert(tagged.contains(WORKER1))
    assert(tagged.contains(WORKER2))
    assert(!tagged.contains(WORKER3))
    assert(tagged.contains(selfTaggedWorker))
  }

  test("getTaggedWorkers matches a worker whose tags span config store and self-registration") {
    tagsManager = new TagsManager(Option(configService))
    // host1 already tagged via config service
    val selfTaggedWorker = workerWithTags("host1", TAG2)
    val all = List(WORKER1, WORKER2, WORKER3, selfTaggedWorker).asJava

    val tagged = tagsManager.getTaggedWorkers(user, "tag1,tag2", all)
    assert(tagged.size == 2)
    assert(tagged.contains(WORKER1))
    assert(tagged.contains(WORKER2))
    assert(!tagged.contains(WORKER3))
  }

  test("getTaggedWorkers returns empty when no config service and workers have no matching tags") {
    tagsManager = new TagsManager(None)
    val untagged = List(workerWithTags("host1")).asJava
    assert(tagsManager.getTaggedWorkers(user, "tag1", untagged).isEmpty)
    assert(tagsManager.getTagsForCluster.isEmpty)
  }
}
