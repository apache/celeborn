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

package org.apache.celeborn.service.deploy.worker

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.protocol.StorageInfo
import org.apache.celeborn.common.protocol.message.StatusCode

class ControllerSuite extends AnyFunSuite {

  private def ids(xs: String*): java.util.List[String] = xs.toList.asJava

  private def set(xs: String*): java.util.Set[String] = {
    val s = ConcurrentHashMap.newKeySet[String]()
    xs.foreach(s.add)
    s
  }

  private def emptyStorageInfos = new ConcurrentHashMap[String, StorageInfo]()
  private def emptyBitMaps = new ConcurrentHashMap[String, org.roaringbitmap.RoaringBitmap]()
  private def emptySizes = new LinkedBlockingQueue[Long]()

  test("CELEBORN-2341: timeout response reports queued/in-flight partitions as failed " +
    "so the driver recomputes them, while preserving committed partitions") {
    val response = Controller.buildCommitFilesResponseOnCancel(
      primaryIds = ids("p0", "p1", "p2", "p3", "p4"),
      replicaIds = ids(),
      committedPrimaryIds = set("p0", "p1"),
      committedReplicaIds = set(),
      emptyFilePrimaryIds = set("p2"),
      emptyFileReplicaIds = set(),
      committedPrimaryStorageInfos = emptyStorageInfos,
      committedReplicaStorageInfos = emptyStorageInfos,
      committedMapIdBitMap = emptyBitMaps,
      partitionSizeList = emptySizes)

    assert(response.status == StatusCode.PARTIAL_SUCCESS)
    assert(response.committedPrimaryIds.asScala.toSet == Set("p0", "p1"))
    assert(response.failedPrimaryIds.asScala.toSet == Set("p3", "p4"))
  }

  test("CELEBORN-2341: timeout response with nothing committed stays COMMIT_FILE_EXCEPTION " +
    "and reports all requested partitions as failed") {
    val response = Controller.buildCommitFilesResponseOnCancel(
      primaryIds = ids("p0", "p1"),
      replicaIds = ids("r0"),
      committedPrimaryIds = set(),
      committedReplicaIds = set(),
      emptyFilePrimaryIds = set(),
      emptyFileReplicaIds = set(),
      committedPrimaryStorageInfos = emptyStorageInfos,
      committedReplicaStorageInfos = emptyStorageInfos,
      committedMapIdBitMap = emptyBitMaps,
      partitionSizeList = emptySizes)

    assert(response.status == StatusCode.COMMIT_FILE_EXCEPTION)
    assert(response.failedPrimaryIds.asScala.toSet == Set("p0", "p1"))
    assert(response.failedReplicaIds.asScala.toSet == Set("r0"))
  }

  test("CELEBORN-2341: empty-file partitions are never reported as failed when nothing " +
    "committed, so they are not needlessly recomputed") {
    // p0/p1 closed empty, p2/p3 were still queued at cancellation, nothing committed.
    val response = Controller.buildCommitFilesResponseOnCancel(
      primaryIds = ids("p0", "p1", "p2", "p3"),
      replicaIds = ids(),
      committedPrimaryIds = set(),
      committedReplicaIds = set(),
      emptyFilePrimaryIds = set("p0", "p1"),
      emptyFileReplicaIds = set(),
      committedPrimaryStorageInfos = emptyStorageInfos,
      committedReplicaStorageInfos = emptyStorageInfos,
      committedMapIdBitMap = emptyBitMaps,
      partitionSizeList = emptySizes)

    assert(response.status == StatusCode.PARTIAL_SUCCESS)
    assert(response.committedPrimaryIds.isEmpty)
    assert(response.failedPrimaryIds.asScala.toSet == Set("p2", "p3"))
  }

  test("CELEBORN-2341: all partitions empty (none committed, none in-flight) reports no " +
    "failures, so the driver does not recompute") {
    val response = Controller.buildCommitFilesResponseOnCancel(
      primaryIds = ids("p0", "p1"),
      replicaIds = ids(),
      committedPrimaryIds = set(),
      committedReplicaIds = set(),
      emptyFilePrimaryIds = set("p0", "p1"),
      emptyFileReplicaIds = set(),
      committedPrimaryStorageInfos = emptyStorageInfos,
      committedReplicaStorageInfos = emptyStorageInfos,
      committedMapIdBitMap = emptyBitMaps,
      partitionSizeList = emptySizes)

    assert(response.failedPrimaryIds.isEmpty)
    assert(response.failedReplicaIds.isEmpty)
  }
}
