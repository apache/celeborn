package org.apache.celeborn.client.commit

import java.util
import java.util.Comparator

import com.google.common.base.Preconditions.{checkArgument, checkState}

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.JavaUtils

class AQEPartitionCompletenessValidator extends Logging {

  private val actualMapperCountForReducer = {
    JavaUtils.newConcurrentHashMap[Int, java.util.TreeMap[(Int, Int), Integer]]()
  }
  private val currentMapperWrittenCountForReducer = JavaUtils.newConcurrentHashMap[Int, Int]()
  private val currentTotalCountForReducer = JavaUtils.newConcurrentHashMap[Int, Int]()
  private val comparator: Comparator[(Int, Int)] = (o1: (Int, Int), o2: (Int, Int)) => {
    val comparator = Integer.compare(o1._1, o2._1)
    if (comparator != 0)
      comparator
    else
      Integer.compare(o1._2, o2._2)
  }

  def validateSubPartition(
      partitionId: Int,
      startMapIndex: Int,
      endMapIndex: Int,
      writtenMapperCount: Int,
      expectedWrittenMapperCountForParent: Int,
      expectedTotalMapperCountForParent: Int): (Boolean, String) = {
    checkArgument(
      startMapIndex < endMapIndex,
      "startMapIndex %s must be less than endMapIndex %s",
      startMapIndex,
      endMapIndex)
    val subrangeToCountMap = actualMapperCountForReducer.computeIfAbsent(
      partitionId,
      _ => new util.TreeMap[(Int, Int), Integer](comparator))
    subrangeToCountMap.synchronized {
      val count = subrangeToCountMap.get((startMapIndex, endMapIndex))
      if (count == null) {
        val (isOverlapping, overlappingEntry) =
          checkOverlappingRange(subrangeToCountMap, startMapIndex, endMapIndex)
        if (isOverlapping) {
          val errorMessage = s"Encountered overlapping map range for partitionId: $partitionId " +
            s" while processing range with startMapIndex: $startMapIndex and endMapIndex: $endMapIndex " +
            s"existing range map: $subrangeToCountMap " +
            s"overlapped with Entry((startMapIndex, endMapIndex), count): $overlappingEntry"
          logError(errorMessage)
          return (false, errorMessage)
        } else {
          subrangeToCountMap.put((startMapIndex, endMapIndex), writtenMapperCount)
          currentMapperWrittenCountForReducer.merge(partitionId, writtenMapperCount, Integer.sum)
          currentTotalCountForReducer.merge(partitionId, endMapIndex - startMapIndex, Integer.sum)
        }
      } else {
        if (count != writtenMapperCount) {
          val errorMessage = s"Written mapper count for partition: $partitionId " +
            s"not matching for sub-partition with startMapIndex: $startMapIndex endMapIndex: $endMapIndex " +
            s"previous count: $count new count: $writtenMapperCount"
          logError(errorMessage)
          return (false, errorMessage)
        }
      }
      return isPartitionValid(
        partitionId,
        expectedWrittenMapperCountForParent,
        expectedTotalMapperCountForParent,
        startMapIndex,
        endMapIndex)
    }
  }

  private def checkOverlappingRange(
      treeMap: java.util.TreeMap[(Int, Int), Integer],
      startMapIndex: Int,
      endMapIndex: Int): (Boolean, ((Int, Int), Int)) = {
    val floorEntry: util.Map.Entry[(Int, Int), Integer] =
      treeMap.floorEntry((startMapIndex, endMapIndex))
    val ceilingEntry: util.Map.Entry[(Int, Int), Integer] =
      treeMap.ceilingEntry((startMapIndex, endMapIndex))

    if (floorEntry != null) {
      if (startMapIndex < floorEntry.getKey._2) {
        return (true, ((floorEntry.getKey._1, floorEntry.getKey._2), floorEntry.getValue))
      }
    }
    if (ceilingEntry != null) {
      if (endMapIndex > ceilingEntry.getKey._1) {
        return (true, ((ceilingEntry.getKey._1, ceilingEntry.getKey._2), ceilingEntry.getValue))
      }
    }
    (false, ((0, 0), 0))
  }

  private def isPartitionValid(
      partitionId: Int,
      expectedWrittenMapperCountForPartition: Int,
      totalMapperCountForPartition: Int,
      startMapIndex: Int,
      endMapIndex: Int): (Boolean, String) = {
    if (!currentTotalCountForReducer.containsKey(partitionId)) {
      checkState(!currentMapperWrittenCountForReducer.containsKey(
        partitionId,
        "mapper total count missing while processing partitionId %s startMapIndex %s endMapIndex %s",
        partitionId,
        startMapIndex,
        endMapIndex))
      currentTotalCountForReducer.put(partitionId, 0)
      currentMapperWrittenCountForReducer.put(partitionId, 0)
    }
    checkState(
      currentMapperWrittenCountForReducer.containsKey(partitionId),
      "mapper written count missing while processing partitionId %s startMapIndex %s endMapIndex %s",
      partitionId,
      startMapIndex,
      endMapIndex)
    val sumOfMapRanges: Int = currentTotalCountForReducer.get(partitionId)
    val sumOfWrittenPartitions: Int =
      currentMapperWrittenCountForReducer.get(partitionId)

    if (sumOfMapRanges > totalMapperCountForPartition) {
      val errorMsg = s"AQE Partition $partitionId failed validation check " +
        s"while processing startMapIndex: $startMapIndex endMapIndex: $endMapIndex " +
        s"ActualWrittenMapperCount $sumOfWrittenPartitions > ExpectedTotalMapperCount $totalMapperCountForPartition"
      logError(errorMsg)
      return (false, errorMsg)
    }
    if (sumOfMapRanges == totalMapperCountForPartition) {
      if (sumOfWrittenPartitions == expectedWrittenMapperCountForPartition) {
        logInfo(
          s"AQE Partition $partitionId completed validation check, " +
            s"WrittenMapperCount $expectedWrittenMapperCountForPartition, " +
            s"totalMapperCount $totalMapperCountForPartition")
        return (true, "Partition is complete")
      } else {

        val entryList = new StringBuilder("Map of range -> actual count\n");
        val mapRangeToCountMap = actualMapperCountForReducer.get(partitionId)
        mapRangeToCountMap.entrySet().forEach(entry => {
          entryList.append(
            "Range " + entry.getKey + " written mapper count: " + entry.getValue + "\n")
        })
        val errorMsg = s"AQE Partition $partitionId failed validation check " +
          s"while processing range startMapIndex: $startMapIndex endMapIndex: $endMapIndex" +
          s"ExpectedWrittenMapperCount $expectedWrittenMapperCountForPartition, " +
          s"ActualWrittenMapperCount $sumOfWrittenPartitions, " +
          s"totalMapperCount $totalMapperCountForPartition, " +
          s"debugInfo ${entryList.toString()}"
        logError(errorMsg)
        return (false, errorMsg)
      }
    }
    (true, "Partition is valid but still waiting for more data")
  }
}
