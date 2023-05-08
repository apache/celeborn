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

package org.apache.celeborn.common.meta

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiFunction

import scala.collection.JavaConverters._

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.util.JavaUtils

class WorkerPartitionLocationInfo extends Logging {

  // key: ShuffleKey, values: (uniqueId -> PartitionLocation))
  type PartitionInfo = ConcurrentHashMap[String, ConcurrentHashMap[String, PartitionLocation]]
  private val masterPartitionLocations = new PartitionInfo
  private val slavePartitionLocations = new PartitionInfo

  def shuffleKeySet: util.HashSet[String] = {
    val shuffleKeySet = new util.HashSet[String]()
    shuffleKeySet.addAll(masterPartitionLocations.keySet())
    shuffleKeySet.addAll(slavePartitionLocations.keySet())
    shuffleKeySet
  }

  def containsShuffle(shuffleKey: String): Boolean = {
    masterPartitionLocations.containsKey(shuffleKey) ||
    slavePartitionLocations.containsKey(shuffleKey)
  }

  def addMasterPartitions(
      shuffleKey: String,
      locations: util.List[PartitionLocation]): Unit = {
    addPartitions(shuffleKey, locations, masterPartitionLocations)
  }

  def addSlavePartitions(
      shuffleKey: String,
      locations: util.List[PartitionLocation]): Unit = {
    addPartitions(shuffleKey, locations, slavePartitionLocations)
  }

  def getMasterLocation(shuffleKey: String, uniqueId: String): PartitionLocation = {
    getLocation(shuffleKey, uniqueId, masterPartitionLocations)
  }

  def getSlaveLocation(shuffleKey: String, uniqueId: String): PartitionLocation = {
    getLocation(shuffleKey, uniqueId, slavePartitionLocations)
  }

  def removeShuffle(shuffleKey: String): Unit = {
    masterPartitionLocations.remove(shuffleKey)
    slavePartitionLocations.remove(shuffleKey)
  }

  def removeMasterPartitions(
      shuffleKey: String,
      uniqueIds: util.Collection[String]): (util.Map[String, Integer], Integer) = {
    removePartitions(shuffleKey, uniqueIds, masterPartitionLocations)
  }

  def removeSlavePartitions(
      shuffleKey: String,
      uniqueIds: util.Collection[String]): (util.Map[String, Integer], Integer) = {
    removePartitions(shuffleKey, uniqueIds, slavePartitionLocations)
  }

  def addPartitions(
      shuffleKey: String,
      locations: util.List[PartitionLocation],
      partitionInfo: PartitionInfo): Unit = {
    if (locations != null && locations.size() > 0) {
      partitionInfo.putIfAbsent(
        shuffleKey,
        JavaUtils.newConcurrentHashMap[String, PartitionLocation]())
      val partitionMap = partitionInfo.get(shuffleKey)
      locations.asScala.foreach { loc =>
        partitionMap.putIfAbsent(loc.getUniqueId, loc)
      }
    }
  }

  /**
   * @param shuffleKey
   * @param uniqueIds
   * @param partitionInfo
   * @return disk related freed slot number and total freed slots number
   */
  private def removePartitions(
      shuffleKey: String,
      uniqueIds: util.Collection[String],
      partitionInfo: PartitionInfo): (util.Map[String, Integer], Integer) = {
    val partitionMap = partitionInfo.get(shuffleKey)
    if (partitionMap == null) {
      return (Map.empty[String, Integer].asJava, 0)
    }
    val locMap = new util.HashMap[String, Integer]()
    var numSlotsReleased: Int = 0
    uniqueIds.asScala.foreach { id =>
      val loc = partitionMap.remove(id)
      if (loc != null) {
        numSlotsReleased += 1
        locMap.compute(
          loc.getStorageInfo.getMountPoint,
          new BiFunction[String, Integer, Integer] {
            override def apply(t: String, u: Integer): Integer = {
              if (u == null) 1 else u + 1
            }
          })
      }
    }

    // some locations might have no disk hint
    (locMap, numSlotsReleased)
  }

  private def getLocation(
      shuffleKey: String,
      uniqueId: String,
      partitionInfo: PartitionInfo): PartitionLocation = {
    val partitionMap = partitionInfo.get(shuffleKey)
    if (partitionMap != null) {
      partitionMap.get(uniqueId)
    } else null
  }

  def isEmpty: Boolean = {
    (masterPartitionLocations.isEmpty ||
      masterPartitionLocations.asScala.values.forall(_.isEmpty)) &&
      (slavePartitionLocations.isEmpty ||
        slavePartitionLocations.asScala.values.forall(_.isEmpty))
  }

  override def toString: String = {
    s"""
       | Partition Location Info:
       | master: ${masterPartitionLocations.asScala}
       | slave: ${slavePartitionLocations.asScala}
       |""".stripMargin
  }
}
