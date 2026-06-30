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

import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.write.LocationPushFailedBatches

private[celeborn] object ReducerFileGroupFilter {

  def fileGroupsForRange(
      allFileGroups: util.Map[Integer, util.Set[PartitionLocation]],
      startPartition: Int,
      endPartition: Int,
      hasPartitionRange: Boolean): util.Map[Integer, util.Set[PartitionLocation]] = {
    if (!hasPartitionRange) {
      return allFileGroups
    }

    val fileGroups = new util.HashMap[Integer, util.Set[PartitionLocation]]()
    var partitionId = startPartition
    while (partitionId < endPartition) {
      val locations = allFileGroups.get(partitionId)
      if (locations != null) {
        fileGroups.put(partitionId, locations)
      }
      partitionId += 1
    }
    fileGroups
  }

  def partitionIdsForRange(
      allPartitionIds: util.Set[Integer],
      startPartition: Int,
      endPartition: Int,
      hasPartitionRange: Boolean): util.Set[Integer] = {
    if (!hasPartitionRange) {
      return new util.HashSet[Integer](allPartitionIds)
    }

    val partitionIds = new util.HashSet[Integer]()
    var partitionId = startPartition
    while (partitionId < endPartition) {
      if (allPartitionIds.contains(partitionId)) {
        partitionIds.add(partitionId)
      }
      partitionId += 1
    }
    partitionIds
  }

  def pushFailedBatchesForFileGroups(
      fileGroups: util.Map[Integer, util.Set[PartitionLocation]],
      allPushFailedBatches: util.Map[String, LocationPushFailedBatches])
      : util.Map[String, LocationPushFailedBatches] = {
    val pushFailedBatches = new util.HashMap[String, LocationPushFailedBatches]()
    fileGroups.values().asScala.foreach(_.asScala.foreach { location =>
      val failedBatches = allPushFailedBatches.get(location.getUniqueId)
      if (failedBatches != null) {
        pushFailedBatches.put(location.getUniqueId, failedBatches)
      }
    })
    pushFailedBatches
  }
}
