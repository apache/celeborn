package org.apache.celeborn.client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.ReviveRequest;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.util.JavaUtils;

public class LocationManager {

  private static final Logger logger = LoggerFactory.getLogger(LocationManager.class);

  private class PartitionLocationList {
    List<PartitionLocation> locations = new ArrayList<>();
    Set<Integer> locationSet = new HashSet<>();
    // epoch id -> status
    Map<Integer, StatusCode> locationStatusCode = new HashMap<>();
    ReviveRequest latestReviveRequest = null;
    int[] index = null;

    int used = 0;
    int size = 0;
    int maxEpoch = -1;

    final int shuffleId;
    final int partitionId;

    public PartitionLocationList(int shuffleId, int partitionId) {
      this.shuffleId = shuffleId;
      this.partitionId = partitionId;
    }

    ReadWriteLock lock = new ReentrantReadWriteLock();
    Lock readLock = lock.readLock();
    Lock writeLock = lock.writeLock();

    private void update(List<PartitionLocation> newLocs) {
      if (newLocs.isEmpty()) {
        return;
      }
      newLocs.sort(Comparator.comparing(PartitionLocation::getEpoch));
      int newMaxEpoch = newLocs.get(newLocs.size() - 1).getEpoch();
      try {
        writeLock.lock();
        if (newMaxEpoch <= maxEpoch) {
          return;
        }
        int newSize = newLocs.size() + size - used;
        ArrayList<PartitionLocation> newLocations = new ArrayList<>(newSize);
        for (PartitionLocation oldLoc : locations) {
          if (locationStatusCode.remove(oldLoc.getEpoch()) != null) {
            used--;
          } else {
            newLocations.add(oldLoc);
          }
        }
        for (PartitionLocation l : newLocs) {
          if (l.getEpoch() >= maxEpoch) {
            newLocations.add(l);
          }
        }
        size = newLocations.size();
        index = new int[size];
        for (int i = 0; i < size; i++) {
          index[i] = i;
        }
        locations = newLocations;
        locationSet.clear();
        locations.forEach(l -> locationSet.add(l.getEpoch()));
        maxEpoch = Math.max(maxEpoch, newMaxEpoch);
        logger.info(
            "Location updated for shuffleId {}, partitionId {}, new locations: {}, maxEpoch: {}",
            shuffleId,
            partitionId,
            locationSet,
            maxEpoch);
        if (latestReviveRequest != null && latestReviveRequest.clientMaxEpoch < maxEpoch) {
          logger.debug("outdated latestReviveRequest {}", latestReviveRequest);
          this.latestReviveRequest = null;
        }
      } finally {
        writeLock.unlock();
      }
    }

    // return partitionLocation for specified mapId
    // if allowSoftSplit = true, soft split location can be returned
    // if liveOnly = false, non-living (soft split/hard split/push fail) location can be returned
    private PartitionLocation nextLoc(int mapId, boolean allowSoftSplit, boolean liveOnly) {
      try {
        readLock.lock();
        int pos = mapId % size;
        int idx = index[pos];
        while (locationStatusCode.get(locations.get(idx).getEpoch()) != null) {
          if (allowSoftSplit
              && locationStatusCode.get(locations.get(idx).getEpoch()) == StatusCode.SOFT_SPLIT) {
            break;
          }
          idx = (idx + 1) % size;
          // all locations are checked
          if (idx == index[pos]) {
            break;
          }
        }
        if (idx != index[pos]) {
          index[pos] = idx;
        }
        if (locationStatusCode.get(locations.get(idx).getEpoch()) != null) {
          if ((allowSoftSplit
                  && locationStatusCode.get(locations.get(idx).getEpoch()) == StatusCode.SOFT_SPLIT)
              || !liveOnly) {
            return locations.get(idx);
          }
          return null;
        } else {
          return locations.get(idx);
        }
      } finally {
        readLock.unlock();
      }
    }

    private void reviveBatch(int shuffleId, int partitionId, int mapId, int attemptId) {
      ReviveRequest reviveRequest = null;
      try {
        writeLock.lock();
        reviveRequest =
            new ReviveRequest(
                shuffleId,
                mapId,
                attemptId,
                partitionId,
                null,
                StatusCode.URGENT_REVIVE,
                maxEpoch,
                true);
        this.latestReviveRequest = reviveRequest;
        logger.debug("in reviveBatch latestReviveRequest = {}", reviveRequest);
      } finally {
        writeLock.unlock();
      }
      reviveManager.addRequest(reviveRequest);
    }

    private void reportUnusableLocation(
        int shuffleId, int mapId, int attemptId, PartitionLocation loc, StatusCode reportedStatus) {
      ReviveRequest reviveRequest = null;
      try {
        writeLock.lock();
        if (!locationSet.contains(loc.getEpoch())) {
          return;
        }

        StatusCode currentStatus = locationStatusCode.get(loc.getEpoch());
        if (currentStatus != reportedStatus) {
          // allow normal/soft split to transition to hard split/push failure
          if (currentStatus == null || currentStatus == StatusCode.SOFT_SPLIT) {
            locationStatusCode.put(loc.getEpoch(), reportedStatus);
          }
          if (currentStatus == null) {
            used++;
          }
          boolean urgent = ((used == size) && !hasActiveReviveRequest());
          reviveRequest =
              new ReviveRequest(
                  shuffleId, mapId, attemptId, loc.getId(), loc, reportedStatus, maxEpoch, urgent);
          if (urgent) {
            this.latestReviveRequest = reviveRequest;
            logger.debug("in reportUnusableLocation latestReviveRequest = {}", reviveRequest);
          }
        }

      } finally {
        writeLock.unlock();
      }
      if (reviveRequest != null) {
        logger.info(
            "Reported worker {}, partitionId {}, epoch {}, shuffle {} map {} attempt {} is unusable, status: {}, urgent: {}",
            loc.hostAndPushPort(),
            loc.getId(),
            loc.getEpoch(),
            shuffleId,
            mapId,
            attemptId,
            reportedStatus.name(),
            reviveRequest.urgent);
        reviveManager.addRequest(reviveRequest);
      }
    }

    private boolean newerPartitionLocationExists(int epoch) {
      try {
        readLock.lock();
        for (PartitionLocation loc : locations) {
          if (locationStatusCode.get(loc.getEpoch()) != null && loc.getEpoch() > epoch) {
            return true;
          }
        }
        return false;
      } finally {
        readLock.unlock();
      }
    }

    private boolean locationExists(int epoch) {
      try {
        readLock.lock();
        return locationSet.contains(epoch);
      } finally {
        readLock.unlock();
      }
    }

    public boolean hasActiveReviveRequest() {
      try {
        readLock.lock();
        return latestReviveRequest != null
            && latestReviveRequest.reviveStatus == StatusCode.REVIVE_INITIALIZED.getValue();
      } finally {
        readLock.unlock();
      }
    }

    public StatusCode getLatestReviveStatus() {
      try {
        readLock.lock();
        if (latestReviveRequest == null) {
          return StatusCode.REVIVE_INITIALIZED;
        } else {
          return StatusCode.fromValue(latestReviveRequest.reviveStatus);
        }
      } finally {
        readLock.unlock();
      }
    }
  }

  final Map<Integer, ConcurrentHashMap<Integer, PartitionLocationList>> reducePartitionMap =
      JavaUtils.newConcurrentHashMap();

  final ReviveManager reviveManager;

  final ShuffleClientImpl shuffleClient;

  public LocationManager(ShuffleClientImpl shuffleClient, ReviveManager reviveManager) {
    this.shuffleClient = shuffleClient;
    this.reviveManager = reviveManager;
  }

  public void registerShuffleLocs(
      int shuffleId, ConcurrentHashMap<Integer, List<PartitionLocation>> map) {
    reducePartitionMap.computeIfAbsent(
        shuffleId,
        (id) -> {
          ConcurrentHashMap<Integer, PartitionLocationList> locationMap =
              JavaUtils.newConcurrentHashMap();
          for (Map.Entry<Integer, List<PartitionLocation>> e : map.entrySet()) {
            int partitionId = e.getKey();
            List<PartitionLocation> locs = e.getValue();
            PartitionLocationList list = new PartitionLocationList(shuffleId, partitionId);
            list.update(locs);
            locationMap.put(partitionId, list);
            logger.debug("in registerShuffleLocs, shuffleId {}, partitionId {}", id, partitionId);
          }
          return locationMap;
        });
  }

  public boolean registered(int shuffleId) {
    return reducePartitionMap.containsKey(shuffleId);
  }

  public boolean exists(int shuffleId, int partitionId) {
    if (!registered(shuffleId)) {
      throw new UnsupportedOperationException(
          "unexpected! must ensure shuffle registered before checking partition exists ");
    }
    return reducePartitionMap.get(shuffleId).containsKey(partitionId);
  }

  public StatusCode getReviveStatus(int shuffleId, int partitionId) {
    PartitionLocationList locationList = reducePartitionMap.get(shuffleId).get(partitionId);
    return locationList.getLatestReviveStatus();
  }

  public PartitionLocation getLocationOrReviveAsync(
      int shuffleId,
      int partitionId,
      int mapId,
      int attemptId,
      boolean doRevive,
      boolean liveOnly) {
    PartitionLocationList locationList = reducePartitionMap.get(shuffleId).get(partitionId);
    // firstly, try to find a live partition location
    PartitionLocation loc = locationList.nextLoc(mapId, false, true);
    if (loc == null) {
      if (doRevive && !locationList.hasActiveReviveRequest()) {
        locationList.reviveBatch(shuffleId, partitionId, mapId, attemptId);
      } else if (doRevive && locationList.hasActiveReviveRequest()) {
        logger.debug(
            "in getLocationOrReviveAsync, do nothing, current latestReviveRequest is {}",
            locationList.latestReviveRequest);
      }
      // can't get a live partition location, then try to find a location in soft split status
      // if liveOnly = false, hard split/push fail location can be returned
      loc = locationList.nextLoc(mapId, true, liveOnly);
    }
    return loc;
  }

  public boolean reviveSync(
      int shuffleId, int partitionId, int mapId, int attemptId, StatusCode cause) {
    Set<Integer> mapIds = new HashSet<>();
    mapIds.add(mapId);
    List<ReviveRequest> requests = new ArrayList<>();
    ReviveRequest request =
        new ReviveRequest(shuffleId, mapId, attemptId, partitionId, null, cause, 0, true);
    requests.add(request);
    Map<Integer, Integer> results = shuffleClient.reviveBatch(shuffleId, mapIds, requests, true);

    if (shuffleClient.mapperEnded(shuffleId, mapId)) {
      logger.debug(
          "Revive success, but the mapper ended for shuffle {} map {} attempt {} partition {}, just return true(Assume revive successfully).",
          shuffleId,
          mapId,
          attemptId,
          partitionId);
      return true;
    } else {
      return results != null
          && results.containsKey(partitionId)
          && results.get(partitionId) == StatusCode.SUCCESS.getValue();
    }
  }

  public void updateLocation(int shuffleId, int partitionId, List<PartitionLocation> newLocations) {
    PartitionLocationList locationList = reducePartitionMap.get(shuffleId).get(partitionId);
    locationList.update(newLocations);
  }

  public void reportUnusableLocation(
      int shuffleId,
      int mapId,
      int attemptId,
      PartitionLocation reportedPartition,
      StatusCode reportedStatus) {
    int partitionId = reportedPartition.getId();
    PartitionLocationList locationList = reducePartitionMap.get(shuffleId).get(partitionId);
    locationList.reportUnusableLocation(
        shuffleId, mapId, attemptId, reportedPartition, reportedStatus);
  }

  public boolean newerPartitionLocationExists(int shuffleId, int partitionId, int epoch) {
    PartitionLocationList locationList = reducePartitionMap.get(shuffleId).get(partitionId);
    return locationList.newerPartitionLocationExists(epoch);
  }

  public boolean locationExists(int shuffleId, int partitionId, int epoch) {
    PartitionLocationList locationList = reducePartitionMap.get(shuffleId).get(partitionId);
    return locationList.locationExists(epoch);
  }

  public boolean hasActiveReviveRequest(int shuffleId, int partitionId) {
    PartitionLocationList locationList = reducePartitionMap.get(shuffleId).get(partitionId);
    return locationList.hasActiveReviveRequest();
  }

  public void removeShuffle(int shuffleId) {
    reducePartitionMap.remove(shuffleId);
  }

  @VisibleForTesting
  public StatusCode getLocationStatus(int shuffleId, int partitionId, int epochId) {
    PartitionLocationList locationList = reducePartitionMap.get(shuffleId).get(partitionId);
    return locationList.locationStatusCode.getOrDefault(epochId, StatusCode.SUCCESS);
  }
}
