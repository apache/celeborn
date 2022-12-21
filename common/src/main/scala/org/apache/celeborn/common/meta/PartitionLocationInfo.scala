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
import java.util.function.BiFunction

import scala.collection.JavaConverters._

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.PartitionLocation

class PartitionLocationInfo extends Logging {

  // key: ShuffleKey, values: (partitionId -> partition locations)
  type PartitionInfo = util.HashMap[String, util.Map[Int, util.List[PartitionLocation]]]
  private val masterPartitionLocations = new PartitionInfo
  private val slavePartitionLocations = new PartitionInfo

  def shuffleKeySet: util.HashSet[String] = {
    val shuffleKeySet = new util.HashSet[String]()
    this.synchronized {
      shuffleKeySet.addAll(masterPartitionLocations.keySet())
      shuffleKeySet.addAll(slavePartitionLocations.keySet())
    }
    shuffleKeySet
  }

  def containsShuffle(shuffleKey: String): Boolean = this.synchronized {
    masterPartitionLocations.containsKey(shuffleKey) ||
    slavePartitionLocations.containsKey(shuffleKey)
  }

  def containsPartition(shuffleKey: String, partitionId: Int): Boolean = this
    .synchronized {
      val contain = masterPartitionLocations.containsKey(
        shuffleKey) && masterPartitionLocations.get(shuffleKey).containsKey(partitionId)
      contain || (slavePartitionLocations.containsKey(shuffleKey) && slavePartitionLocations.get(
        shuffleKey)
        .containsKey(partitionId))
    }

  def addMasterPartition(shuffleKey: String, location: PartitionLocation): Int = {
    addPartition(shuffleKey, location, masterPartitionLocations)
  }

  def addSlavePartition(shuffleKey: String, location: PartitionLocation): Int = {
    addPartition(shuffleKey, location, slavePartitionLocations)
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
    getLocation(shuffleKey, uniqueId, PartitionLocation.Mode.MASTER)
  }

  def getSlaveLocation(shuffleKey: String, uniqueId: String): PartitionLocation = {
    getLocation(shuffleKey, uniqueId, PartitionLocation.Mode.SLAVE)
  }

  def getAllMasterLocations(shuffleKey: String): util.List[PartitionLocation] = this.synchronized {
    getMasterLocations(shuffleKey)
  }

  def getAllSlaveLocations(shuffleKey: String): util.List[PartitionLocation] = this.synchronized {
    getSlaveLocations(shuffleKey)
  }

  def getMasterLocations(
      shuffleKey: String,
      partitionIdOpt: Option[Int] = None): util.List[PartitionLocation] = {
    getLocations(shuffleKey, masterPartitionLocations, partitionIdOpt)
  }

  def getSlaveLocations(
      shuffleKey: String,
      partitionIdOpt: Option[Int] = None): util.List[PartitionLocation] = {
    getLocations(shuffleKey, slavePartitionLocations, partitionIdOpt)
  }

  private def getLocations(
      shuffleKey: String,
      partitionInfo: PartitionInfo,
      partitionIdOpt: Option[Int] = None): util.List[PartitionLocation] = this.synchronized {
    if (partitionInfo.containsKey(shuffleKey)) {
      partitionIdOpt match {
        case Some(partitionId) => partitionInfo.get(shuffleKey)
            .values()
            .asScala
            .flatMap(_.asScala)
            .filter(_.getId == partitionId)
            .toList.asJava
        case None =>
          partitionInfo.get(shuffleKey)
            .values()
            .asScala
            .flatMap(_.asScala)
            .toList.asJava
      }
    } else {
      new util.ArrayList[PartitionLocation]()
    }
  }

  def removeMasterPartitions(shuffleKey: String): (util.Map[String, Integer], Integer) = {
    val uniqueIds = getAllMasterIds(shuffleKey)
    removeMasterPartitions(shuffleKey, uniqueIds)
  }

  def removeSlavePartitions(shuffleKey: String): (util.Map[String, Integer], Integer) = {
    val uniqueIds = getAllSlaveIds(shuffleKey)
    removeSlavePartitions(shuffleKey, uniqueIds)
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

  def getAllMasterLocationsWithMinEpoch(shuffleKey: String): util.List[PartitionLocation] =
    this.synchronized {
      getAllMasterLocationsWithExtremeEpoch(shuffleKey, (a, b) => a < b)
    }

  def getAllMasterLocationsWithExtremeEpoch(
      shuffleKey: String,
      order: (Int, Int) => Boolean): util.List[PartitionLocation] = this.synchronized {
    if (masterPartitionLocations.containsKey(shuffleKey)) {
      masterPartitionLocations.get(shuffleKey)
        .values()
        .asScala
        .map { list =>
          var loc = list.get(0)
          1 until list.size() foreach (ind => {
            if (order(list.get(ind).getEpoch, loc.getEpoch)) {
              loc = list.get(ind)
            }
          })
          loc
        }.toList.asJava
    } else {
      new util.ArrayList[PartitionLocation]()
    }
  }

  def getLocationWithMaxEpoch(
      shuffleKey: String,
      partitionId: Int): Option[PartitionLocation] = this.synchronized {
    if (!masterPartitionLocations.containsKey(shuffleKey) ||
      !masterPartitionLocations.get(shuffleKey).containsKey(partitionId)) {
      return None
    }
    val locations = masterPartitionLocations.get(shuffleKey).get(partitionId)
    if (locations == null || locations.size() == 0) {
      return None
    }
    var currentEpoch = -1
    var currentPartition: PartitionLocation = null
    locations.asScala.foreach(loc => {
      if (loc.getEpoch > currentEpoch) {
        currentEpoch = loc.getEpoch
        currentPartition = loc
      }
    })
    Some(currentPartition)
  }

  private def addPartition(
      shuffleKey: String,
      location: PartitionLocation,
      partitionInfo: PartitionInfo): Int = this.synchronized {
    if (location != null) {
      partitionInfo.putIfAbsent(shuffleKey, new util.HashMap[Int, util.List[PartitionLocation]]())
      val reduceLocMap = partitionInfo.get(shuffleKey)
      reduceLocMap.putIfAbsent(location.getId, new util.ArrayList[PartitionLocation]())
      val locations = reduceLocMap.get(location.getId)
      locations.add(location)
      1
    } else {
      0
    }
  }

  private def addPartitions(
      shuffleKey: String,
      locations: util.List[PartitionLocation],
      partitionInfo: PartitionInfo): Unit = this.synchronized {
    if (locations != null && locations.size() > 0) {
      partitionInfo.putIfAbsent(shuffleKey, new util.HashMap[Int, util.List[PartitionLocation]]())
      val reduceLocMap = partitionInfo.get(shuffleKey)
      locations.asScala.foreach { loc =>
        reduceLocMap.putIfAbsent(loc.getId, new util.ArrayList[PartitionLocation]())
        val locations = reduceLocMap.get(loc.getId)
        locations.add(loc)
      }
    }
  }

  def removeRelatedPartitions(
      shuffleKey: String,
      partitionId: Int): Unit = this
    .synchronized {
      if (masterPartitionLocations.containsKey(shuffleKey)) {
        masterPartitionLocations.get(shuffleKey).remove(partitionId)
      }
      if (slavePartitionLocations.containsKey(shuffleKey)) {
        slavePartitionLocations.get(shuffleKey).remove(partitionId)
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
      partitionInfo: PartitionInfo): (util.Map[String, Integer], Integer) = this.synchronized {
    if (!partitionInfo.containsKey(shuffleKey)) {
      return (Map.empty[String, Integer].asJava, 0)
    }
    val locMap = new util.HashMap[String, Integer]()
    var numSlotsReleased: Int = 0
    val reduceLocMap = partitionInfo.get(shuffleKey)
    uniqueIds.asScala.foreach { id =>
      val tokens = id.split("-", 2)
      val partitionId = tokens(0).toInt
      val epoch = tokens(1).toInt
      val locations = reduceLocMap.get(partitionId)
      if (locations != null) {
        val targetLocation = locations.asScala.find(_.getEpoch == epoch).orNull
        if (targetLocation != null) {
          locations.remove(targetLocation)
          numSlotsReleased += 1
          locMap.compute(
            targetLocation.getStorageInfo.getMountPoint,
            new BiFunction[String, Integer, Integer] {
              override def apply(t: String, u: Integer): Integer = {
                if (u == null) 1 else u + 1
              }
            })
        }
      }
      if (locations == null || locations.size() == 0) {
        reduceLocMap.remove(partitionId)
      }
    }

    if (reduceLocMap.size() == 0) {
      partitionInfo.remove(shuffleKey)
    }
    // some locations might have no disk hint
    (locMap, numSlotsReleased)
  }

  private def getLocation(
      shuffleKey: String,
      uniqueId: String,
      mode: PartitionLocation.Mode): PartitionLocation = {
    val tokens = uniqueId.split("-", 2)
    val partitionId = tokens(0).toInt
    val epoch = tokens(1).toInt
    val partitionInfo =
      if (mode == PartitionLocation.Mode.MASTER) {
        masterPartitionLocations
      } else {
        slavePartitionLocations
      }

    this.synchronized {
      if (!partitionInfo.containsKey(shuffleKey)
        || !partitionInfo.get(shuffleKey).containsKey(partitionId)) {
        return null
      }
      partitionInfo.get(shuffleKey)
        .get(partitionId)
        .asScala
        .find(loc => loc.getEpoch == epoch)
        .orNull
    }
  }

  private def getAllIds(
      shuffleKey: String,
      partitionInfo: PartitionInfo): util.List[String] = this.synchronized {
    if (!partitionInfo.containsKey(shuffleKey)) {
      return null
    }
    partitionInfo.get(shuffleKey)
      .values()
      .asScala
      .flatMap(_.asScala)
      .map(_.getUniqueId)
      .toList
      .asJava
  }

  private def getAllMasterIds(shuffleKey: String): util.List[String] = {
    getAllIds(shuffleKey, masterPartitionLocations)
  }

  private def getAllSlaveIds(shuffleKey: String): util.List[String] = {
    getAllIds(shuffleKey, slavePartitionLocations)
  }

  def isEmpty: Boolean = this.synchronized {
    masterPartitionLocations.isEmpty && slavePartitionLocations.isEmpty
  }

  override def toString: String = this.synchronized {
    s"""
       | Partition Location Info:
       | master: ${masterPartitionLocations.asScala}
       | slave: ${slavePartitionLocations.asScala}
       |""".stripMargin
  }
}
