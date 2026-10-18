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

package org.apache.celeborn.client.commit

import java.util

import scala.collection.JavaConverters._

import org.mockito.Mockito

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.{PartitionLocation, StorageInfo}
import org.apache.celeborn.common.protocol.StorageInfo.Type
import org.apache.celeborn.common.rpc.RpcEndpointRef

class HdfsReplicaReleaseSuite extends CelebornFunSuite {

  private val replicaWorker = worker()

  private def worker(withEndpoint: Boolean = true): WorkerInfo = {
    val w = new WorkerInfo("replica-host", 1, 2, 3, 4)
    if (withEndpoint) {
      w.endpoint = Mockito.mock(classOf[RpcEndpointRef])
    }
    w
  }

  private def storageInfos(id: String, t: Option[Type]): util.Map[String, StorageInfo] =
    t.map(storageType => id -> new StorageInfo(storageType, true, "")).toMap.asJava

  private case class Case(
      name: String,
      primaryType: Option[Type],
      replicaType: Option[Type],
      allocatedWorker: Option[WorkerInfo] = Some(replicaWorker),
      primaryCommitted: Boolean = true,
      replicaCommitted: Boolean = true,
      expectReleased: Boolean = false)

  Seq(
    Case("both HDFS", Some(Type.HDFS), Some(Type.HDFS), expectReleased = true),
    Case("primary not committed", Some(Type.HDFS), Some(Type.HDFS), primaryCommitted = false),
    Case("replica not committed", Some(Type.HDFS), Some(Type.HDFS), replicaCommitted = false),
    Case("primary MEMORY, replica HDFS", Some(Type.MEMORY), Some(Type.HDFS)),
    Case("primary HDFS, replica MEMORY", Some(Type.HDFS), Some(Type.MEMORY)),
    Case("both S3", Some(Type.S3), Some(Type.S3)),
    Case("both OSS", Some(Type.OSS), Some(Type.OSS)),
    Case("primary storage info missing", None, Some(Type.HDFS)),
    Case("replica storage info missing", Some(Type.HDFS), None),
    Case("replica worker not allocated", Some(Type.HDFS), Some(Type.HDFS), None),
    Case(
      "replica worker has no endpoint",
      Some(Type.HDFS),
      Some(Type.HDFS),
      Some(worker(withEndpoint = false)))).foreach { c =>
    test(s"selectHdfsReplicasToRelease: ${c.name}") {
      val replica =
        new PartitionLocation(0, 0, "replica-host", 1, 2, 3, 4, PartitionLocation.Mode.REPLICA)
      val id = replica.getUniqueId
      val workers = c.allocatedWorker.map(w =>
        w.toUniqueId -> new ShufflePartitionLocationInfo(w)).toMap.asJava
      val slots = CommitHandler.selectHdfsReplicasToRelease(
        if (c.primaryCommitted) Set(id) else Set.empty,
        if (c.replicaCommitted) Seq(id) else Seq.empty,
        storageInfos(id, c.primaryType),
        storageInfos(id, c.replicaType),
        Map(id -> replica).asJava,
        workers)

      if (c.expectReleased) {
        assert(slots.size === 1)
        val (slotWorker, (primaries, replicas)) = slots.asScala.head
        assert(slotWorker eq replicaWorker)
        assert(primaries.isEmpty && replicas.asScala === Seq(replica))
      } else {
        assert(slots.isEmpty)
      }
    }
  }
}
