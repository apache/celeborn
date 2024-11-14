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
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.server.common.service.config.DynamicConfigServiceFactory

class TagsManagerSuite extends CelebornFunSuite {
  private var tagsManager: TagsManager = _

  private val TAG1 = "tag1"
  private val TAG2 = "tag2"

  private val WORKER1 = new WorkerInfo("host1", 111, 112, 113, 114, 115)
  private val WORKER2 = new WorkerInfo("host2", 211, 212, 213, 214, 215)
  private val WORKER3 = new WorkerInfo("host3", 311, 312, 313, 314, 315)

  private val workers = List(WORKER1, WORKER2, WORKER3).asJava

  override def beforeEach(): Unit = {
    super.beforeEach()
    DynamicConfigServiceFactory.reset()
  }

  test("test tags manager") {
    tagsManager = new TagsManager(Option(null))

    tagsManager.addTagToWorker(TAG1, WORKER1.toUniqueId())
    tagsManager.addTagToWorker(TAG1, WORKER2.toUniqueId())

    tagsManager.addTagToWorker(TAG2, WORKER2.toUniqueId())
    tagsManager.addTagToWorker(TAG2, WORKER3.toUniqueId())

    {
      val taggedWorkers = tagsManager.getTaggedWorkers(TAG1, workers)
      assert(taggedWorkers.size == 2)
      assert(taggedWorkers.contains(WORKER1))
      assert(taggedWorkers.contains(WORKER2))
      assert(!taggedWorkers.contains(WORKER3))
    }

    {
      val taggedWorkers = tagsManager.getTaggedWorkers(TAG2, workers)
      assert(taggedWorkers.size == 2)
      assert(!taggedWorkers.contains(WORKER1))
      assert(taggedWorkers.contains(WORKER2))
      assert(taggedWorkers.contains(WORKER3))
    }

    {
      // Test get tags for cluster
      val tags = tagsManager.getTagsForCluster
      assert(tags.size == 2)
      assert(tags.contains(TAG1))
      assert(tags.contains(TAG2))
    }

    {
      // Test an unknown tag
      val taggedWorkers = tagsManager.getTaggedWorkers("unknown-tag", workers)
      assert(taggedWorkers.isEmpty)
    }

    {
      // Test get tags for worker
      val tagsWorker1 = tagsManager.getTagsForWorker(WORKER1)
      assert(tagsWorker1.size == 1)
      assert(tagsWorker1.contains(TAG1))

      val tagsWorker2 = tagsManager.getTagsForWorker(WORKER2)
      assert(tagsWorker2.size == 2)
      assert(tagsWorker2.contains(TAG1))
      assert(tagsWorker2.contains(TAG2))

      val tagsWorker3 = tagsManager.getTagsForWorker(WORKER3)
      assert(tagsWorker3.size == 1)
      assert(tagsWorker3.contains(TAG2))

      // Untagged worker
      val untaggedWorker = new WorkerInfo("host4", 999, 999, 999, 999, 999)
      val tagsUntaggedWorker = tagsManager.getTagsForWorker(untaggedWorker)
      assert(tagsUntaggedWorker.isEmpty)
    }

    {
      // Remove tag from worker
      tagsManager.removeTagFromWorker(TAG1, WORKER2.toUniqueId())
      val taggedWorkers = tagsManager.getTaggedWorkers(TAG1, workers)
      assert(taggedWorkers.size == 1)
      assert(taggedWorkers.contains(WORKER1))
      assert(!taggedWorkers.contains(WORKER2))
      assert(!taggedWorkers.contains(WORKER3))
    }

    {
      // Remove tag from cluster
      tagsManager.removeTagFromCluster(TAG1)
      val taggedWorkers = tagsManager.getTaggedWorkers(TAG1, workers)
      assert(taggedWorkers.isEmpty)

      val tags = tagsManager.getTagsForCluster
      assert(tags.size == 1)
      assert(tags.contains(TAG2))
    }
  }

  test("test tags expression with multiple tags") {
    tagsManager = new TagsManager(Option(null))

    // Tag1
    tagsManager.addTagToWorker(TAG1, WORKER1.toUniqueId())
    tagsManager.addTagToWorker(TAG1, WORKER2.toUniqueId())

    // Tag2
    tagsManager.addTagToWorker(TAG2, WORKER2.toUniqueId())
    tagsManager.addTagToWorker(TAG2, WORKER3.toUniqueId())

    {
      val taggedWorkers = tagsManager.getTaggedWorkers("tag1,tag2", workers)
      assert(taggedWorkers.size == 1)
      assert(!taggedWorkers.contains(WORKER1))
      assert(taggedWorkers.contains(WORKER2))
      assert(!taggedWorkers.contains(WORKER3))
    }

    {
      val taggedWorkers = tagsManager.getTaggedWorkers("tag1,tag3", workers)
      assert(taggedWorkers.size == 0)
    }
  }

  test("test tags manager with config service") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.DYNAMIC_CONFIG_STORE_BACKEND, "FS")
    conf.set(
      CelebornConf.DYNAMIC_CONFIG_STORE_FS_PATH.key,
      getTestResourceFile("dynamicConfig-tags.yaml").getPath)
    val configService = DynamicConfigServiceFactory.getConfigService(conf)

    tagsManager = new TagsManager(Option(configService))

    {
      val taggedWorkers = tagsManager.getTaggedWorkers(TAG1, workers)
      assert(taggedWorkers.size == 2)
      assert(taggedWorkers.contains(WORKER1))
      assert(taggedWorkers.contains(WORKER2))
      assert(!taggedWorkers.contains(WORKER3))
    }

    {
      val taggedWorkers = tagsManager.getTaggedWorkers(TAG2, workers)
      assert(taggedWorkers.size == 2)
      assert(!taggedWorkers.contains(WORKER1))
      assert(taggedWorkers.contains(WORKER2))
      assert(taggedWorkers.contains(WORKER3))
    }
  }
}
