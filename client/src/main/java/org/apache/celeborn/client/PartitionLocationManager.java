package org.apache.celeborn.client;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;

import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.util.JavaUtils;

public class PartitionLocationManager {

  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final Lock readLock = rwLock.readLock();
  private final Lock writeLock = rwLock.writeLock();
  private static final Random random = new Random();
  private Map<PartitionLocation, ArrayList<PartitionLocation>> locationTree =
      JavaUtils.newConcurrentHashMap();

  private final AtomicInteger epochCounter = new AtomicInteger(0);

  public int incrementAndGetEpoch() {
    return epochCounter.incrementAndGet();
  }

  public void initLocs(PartitionLocation partitionLocation) {
    locationTree.put(partitionLocation, new ArrayList<>());
  }

  @VisibleForTesting
  public List<PartitionLocation> getLeafLocations(PartitionLocation initLoc) {
    readLock.lock();
    try {
      if (initLoc == null) {
        return new ArrayList<>(locationTree.keySet());
      }
      LinkedList<PartitionLocation> queue = new LinkedList<>();
      queue.add(initLoc);
      List<PartitionLocation> leafLocations = new ArrayList<>();
      while (!queue.isEmpty()) {
        PartitionLocation curr = queue.removeFirst();
        List<PartitionLocation> currChildren = locationTree.get(curr);
        if (currChildren == null || currChildren.isEmpty()) {
          leafLocations.add(curr);
        } else {
          List<PartitionLocation> children = new ArrayList<>(currChildren);
          children.sort(Comparator.comparingInt(PartitionLocation::getSplitStart));
          for (PartitionLocation child : children) {
            queue.addLast(child);
          }
        }
      }
      leafLocations.sort(Comparator.comparingInt(PartitionLocation::getSplitStart));
      return leafLocations;
    } finally {
      readLock.unlock();
    }
  }

  public void addChildren(PartitionLocation parent, List<PartitionLocation> newChildren)
      throws CelebornIOException {
    writeLock.lock();
    try {
      if (locationTree.get(parent) == null) {
        throw new CelebornIOException("unknown partition location " + parent);
      }
      List<PartitionLocation> children = locationTree.get(parent);
      children.addAll(newChildren);
    } finally {
      writeLock.unlock();
    }
  }

  public void removeChild(PartitionLocation parent, PartitionLocation child) {
    writeLock.lock();
    try {
      if (locationTree.get(parent) != null) {
        List<PartitionLocation> children = locationTree.get(parent);
        children.remove(child);
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void setChildren(PartitionLocation parent, List<PartitionLocation> targetChildren) {
    writeLock.lock();
    try {
      locationTree.putIfAbsent(parent, new ArrayList<>());
      List<PartitionLocation> children = locationTree.get(parent);
      children.clear();
      children.addAll(targetChildren);
    } finally {
      writeLock.unlock();
    }
  }

  public PartitionLocation getLatestPartitionLocation(PartitionLocation parent) {
    readLock.lock();
    try {
      if (parent == null) { // if partitionType is MAP
        return null;
      }
      PartitionLocation curr = parent;
      while (locationTree.get(curr) != null && !locationTree.get(curr).isEmpty()) {
        List<PartitionLocation> children = locationTree.get(curr);
        curr = children.get(random.nextInt(children.size()));
      }
      return curr.getEpoch() > parent.getEpoch() ? curr : null;
    } finally {
      readLock.unlock();
    }
  }

  public PartitionLocation getRandomChild(PartitionLocation parent) {
    if (parent == null) { // if partitionType is MAP
      return (PartitionLocation) locationTree.keySet().toArray()[0];
    }
    List<PartitionLocation> children = locationTree.get(parent);
    if (children != null && !children.isEmpty()) {
      return children.get(random.nextInt(children.size()));
    }
    return null;
  }

  @VisibleForTesting
  public List<PartitionLocation> getChildren(PartitionLocation parent) {
    readLock.lock();
    try {
      return locationTree.get(parent);
    } finally {
      readLock.unlock();
    }
  }

  public List<SplitInfo> split(int splitNum, PartitionLocation oldPartition, boolean updateEpoch) {
    List<SplitInfo> splitInfos = new ArrayList<>();
    int parentSplitStart = oldPartition.getSplitStart();
    int parentSplitEnd = oldPartition.getSplitEnd();
    int parentSplitRangeLen = parentSplitEnd - parentSplitStart + 1;
    int newSplitNum = Math.min(parentSplitRangeLen, splitNum);
    int baseRangeLen = parentSplitRangeLen / newSplitNum;
    int remainder = parentSplitRangeLen % newSplitNum;
    for (int splitIdx = 0; splitIdx < newSplitNum; splitIdx++) {
      int rangeLen = splitIdx < remainder ? baseRangeLen + 1 : baseRangeLen;
      int splitStart = parentSplitStart + splitIdx * baseRangeLen + Math.min(splitIdx, remainder);
      int splitEnd = splitStart + rangeLen - 1;
      int newEpoch = updateEpoch ? incrementAndGetEpoch() : oldPartition.getEpoch();
      splitInfos.add(new SplitInfo(oldPartition.getId(), newEpoch, splitStart, splitEnd));
    }
    return splitInfos;
  }
}
