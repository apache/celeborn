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

package org.apache.celeborn.client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.StatusCode;

/**
 * A thin wrapper of the writable PartitionLocation(s) of one (shuffleId, partitionId).
 *
 * <p>In the common case (the partition never splits) it only holds a single volatile {@link
 * PartitionLocation} reference, so the fast path costs exactly one extra object header compared to
 * storing the location directly. The parallel state (active location list and retired epochs) is
 * inflated lazily on the first SOFT_SPLIT/HARD_SPLIT/push failure, or when a revive response
 * delivers more than one active location.
 *
 * <p>Selection policy: uniform over the writable subset — non-retired and SOFT_SPLIT locations (a
 * soft-split file stays writable until it hard-splits) — via {@code mapId % writableCount}.
 * Hard-retired (HARD_SPLIT / push failure) locations are skipped; writes to them are re-pushed to
 * another writable location.
 */
public class PartitionLocationGroup {

  /**
   * The only location of this partition before inflation. Only read on the non-inflated fast path
   * (and as {@link #latest()}'s fallback when the inflated active list is empty); it is
   * intentionally NOT kept in sync after inflation — the inflated {@link ParallelState#active} list
   * is the source of truth.
   */
  private volatile PartitionLocation single;

  /** null until inflated; all mutating methods go through double-checked inflation. */
  private volatile ParallelState parallel;

  public PartitionLocationGroup(PartitionLocation loc) {
    this.single = loc;
  }

  /** Fast path: returns the single location when not inflated, zero extra cost. */
  public PartitionLocation currentFor(int mapId) {
    return pick(mapId, -1);
  }

  /**
   * Pick a writable location for {@code mapId}, skipping {@code excludeEpoch} (-1 = skip nothing).
   * Writable means non-retired or soft-split: a SOFT_SPLIT file stays writable until it grows to
   * partitionSplitMaximumSize and hard-splits, so soft-split locations remain first-class routing
   * targets — this keeps the write load truly spread over all writable locations instead of
   * collapsing onto the few non-retired ones. Traffic is spread uniformly over the writable subset
   * via {@code mapId % writableCount}. Returns null when nothing is writable.
   */
  private PartitionLocation pick(int mapId, int excludeEpoch) {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation loc = single;
      return (loc != null && loc.getEpoch() != excludeEpoch) ? loc : null;
    }
    // Snapshot the active list: mergeActiveLocations(fullSet=true) may concurrently shrink it
    // via removeIf, so size()-then-get() on the live list races and can go out of bounds.
    PartitionLocation[] snapshot = p.active.toArray(new PartitionLocation[0]);
    int writable = 0;
    for (PartitionLocation loc : snapshot) {
      if (isWritable(p, loc, excludeEpoch)) {
        writable++;
      }
    }
    if (writable == 0) {
      return null;
    }
    int start = Math.floorMod(mapId, writable);
    for (PartitionLocation loc : snapshot) {
      if (isWritable(p, loc, excludeEpoch) && start-- == 0) {
        return loc;
      }
    }
    return null;
  }

  private static boolean isWritable(ParallelState p, PartitionLocation loc, int excludeEpoch) {
    if (loc.getEpoch() == excludeEpoch) {
      return false;
    }
    StatusCode cause = p.retired.get(loc.getEpoch());
    return cause == null || cause == StatusCode.SOFT_SPLIT;
  }

  /** The location with the max epoch, used where a single representative location is needed. */
  public PartitionLocation latest() {
    ParallelState p = parallel;
    if (p == null) {
      return single;
    }
    // Snapshot for the same reason as pick(): a concurrent full-set eviction may shrink the
    // list between the emptiness check and the last-element access.
    PartitionLocation[] snapshot = p.active.toArray(new PartitionLocation[0]);
    return snapshot.length == 0 ? single : snapshot[snapshot.length - 1];
  }

  public int maxEpoch() {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation loc = single;
      return loc == null ? -1 : loc.getEpoch();
    }
    return p.maxEpoch;
  }

  /** Whether at least one location can still accept writes (active or soft-split). */
  public boolean hasUsable() {
    ParallelState p = parallel;
    if (p == null) {
      return single != null;
    }
    for (PartitionLocation loc : p.active) {
      StatusCode cause = p.retired.get(loc.getEpoch());
      if (cause == null || cause == StatusCode.SOFT_SPLIT) {
        return true;
      }
    }
    return false;
  }

  /**
   * Pick a usable location other than {@code excludeEpoch} for {@code mapId}, used to re-push
   * batches without waiting for revive when the partition has more than one usable location.
   * Returns null if there is no other usable location.
   */
  public PartitionLocation anotherUsableFor(int mapId, int excludeEpoch) {
    return pick(mapId, excludeEpoch);
  }

  /**
   * A retire report for a locally retired location that the LifecycleManager may not have digested
   * yet — the location is still held in the active list, i.e. the LM most likely still counts its
   * epoch as surviving and keeps reporting it in full-set responses. Forwarding these reports with
   * a revive lets the LM shrink its active set to the truth so gap allocation produces a fresh
   * location instead of re-replying with epochs this executor has already retired (the
   * NULL-location failure path).
   */
  public static final class OutstandingRetire {
    public final PartitionLocation location;
    public final StatusCode cause;

    OutstandingRetire(PartitionLocation location, StatusCode cause) {
      this.location = location;
      this.cause = cause;
    }
  }

  /**
   * Snapshot of {@link OutstandingRetire}s, epoch ascending. Only epochs still present in the
   * active list are included: an evicted epoch (no longer reported by the LM) has already been
   * digested, and its location object is gone, so there is nothing left to report.
   */
  public List<OutstandingRetire> outstandingRetires() {
    ParallelState p = parallel;
    if (p == null) {
      return new ArrayList<>(0);
    }
    List<OutstandingRetire> retires = new ArrayList<>();
    for (PartitionLocation loc : p.active.toArray(new PartitionLocation[0])) {
      StatusCode cause = p.retired.get(loc.getEpoch());
      if (cause != null) {
        retires.add(new OutstandingRetire(loc, cause));
      }
    }
    return retires;
  }

  /**
   * Mark {@code epoch} as retired with {@code cause}. Soft-retired locations stay writable and
   * remain routing targets; hard-retired ones are skipped by {@link #currentFor(int)}. A later
   * non-soft cause upgrades a previous SOFT_SPLIT retire (the location hard-split or failed
   * afterwards), so it stops being writable; a harder cause is never downgraded back to SOFT_SPLIT.
   *
   * @return true if this is the first time the epoch is retired (dedupe signal for the caller to
   *     send at most one revive per epoch).
   */
  public boolean retire(int epoch, StatusCode cause) {
    ParallelState p = inflateIfNeeded();
    boolean[] firstRetire = {false};
    p.retired.compute(
        epoch,
        (k, existing) -> {
          if (existing == null) {
            firstRetire[0] = true;
            return cause;
          }
          return existing == StatusCode.SOFT_SPLIT && cause != StatusCode.SOFT_SPLIT
              ? cause
              : existing;
        });
    return firstRetire[0];
  }

  /**
   * Converge to the active set delivered by the LifecycleManager: add locally missing epochs, never
   * re-add locally retired ones. When {@code fullSet} is true the list is the LM's full active set,
   * so retired epochs that the LM no longer reports (it has processed the retirement and allocated
   * replacements) are dropped from both the active list and the retired map — this bounds the list
   * size and keeps {@code mapId}-based routing uniform among live locations. Synchronized because
   * the check-then-add of {@link #insertActive} is not atomic across concurrent revive responses.
   */
  public synchronized void mergeActiveLocations(
      List<PartitionLocation> locations, boolean fullSet) {
    if (locations == null || locations.isEmpty()) {
      return;
    }
    if (parallel == null && locations.size() == 1) {
      // Stay in thin-wrapper mode when the LM only knows one active location.
      updateLatest(locations.get(0));
      return;
    }
    ParallelState p = inflateIfNeeded();
    for (PartitionLocation loc : locations) {
      if (loc == null || p.retired.containsKey(loc.getEpoch())) {
        continue;
      }
      insertActive(p, loc);
      if (loc.getEpoch() > p.maxEpoch) {
        p.maxEpoch = loc.getEpoch();
      }
    }
    if (fullSet && !p.retired.isEmpty()) {
      Set<Integer> reported = new HashSet<>();
      for (PartitionLocation loc : locations) {
        if (loc != null) {
          reported.add(loc.getEpoch());
        }
      }
      p.active.removeIf(
          loc -> p.retired.containsKey(loc.getEpoch()) && !reported.contains(loc.getEpoch()));
      p.retired.keySet().removeIf(epoch -> !reported.contains(epoch));
    }
  }

  /**
   * Legacy single-location update (adaptive parallelism disabled or single-location response).
   * Synchronized because revive responses can be applied concurrently by the ReviveManager
   * scheduler thread and by push threads via the blocking revive path, so the non-inflated
   * check-then-set on {@link #single} must be atomic.
   */
  public synchronized void updateLatest(PartitionLocation loc) {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation cur = single;
      if (cur == null || loc.getEpoch() >= cur.getEpoch()) {
        single = loc;
      }
    } else {
      List<PartitionLocation> one = new ArrayList<>(1);
      one.add(loc);
      mergeActiveLocations(one, false);
    }
  }

  public boolean hasParallelState() {
    return parallel != null;
  }

  /** Visible for testing. */
  int activeCount() {
    ParallelState p = parallel;
    return p == null ? (single == null ? 0 : 1) : p.active.size();
  }

  /** Visible for testing and observability logging. */
  int retiredCount() {
    ParallelState p = parallel;
    return p == null ? 0 : p.retired.size();
  }

  /** Epochs of the active list, ascending — for diagnostics in failure messages. */
  List<Integer> activeEpochsSnapshot() {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation loc = single;
      List<Integer> epochs = new ArrayList<>(1);
      if (loc != null) {
        epochs.add(loc.getEpoch());
      }
      return epochs;
    }
    List<Integer> epochs = new ArrayList<>(p.active.size());
    for (PartitionLocation loc : p.active) {
      epochs.add(loc.getEpoch());
    }
    return epochs;
  }

  /** Retired epochs and their causes, for diagnostics in failure messages. */
  List<String> retiredEpochsSnapshot() {
    ParallelState p = parallel;
    if (p == null) {
      return new ArrayList<>(0);
    }
    List<String> retires = new ArrayList<>(p.retired.size());
    for (Map.Entry<Integer, StatusCode> entry : p.retired.entrySet()) {
      retires.add(entry.getKey() + "=" + entry.getValue());
    }
    retires.sort(Comparator.comparing(s -> Integer.parseInt(s.substring(0, s.indexOf('=')))));
    return retires;
  }

  private ParallelState inflateIfNeeded() {
    ParallelState p = parallel;
    if (p == null) {
      synchronized (this) {
        p = parallel;
        if (p == null) {
          p = new ParallelState();
          PartitionLocation loc = single;
          if (loc != null) {
            p.active.add(loc);
            p.maxEpoch = loc.getEpoch();
          }
          parallel = p;
        }
      }
    }
    return p;
  }

  private static void insertActive(ParallelState p, PartitionLocation loc) {
    List<PartitionLocation> active = p.active;
    int i = 0;
    while (i < active.size() && active.get(i).getEpoch() < loc.getEpoch()) {
      i++;
    }
    if (i < active.size() && active.get(i).getEpoch() == loc.getEpoch()) {
      // Already present (location objects are immutable per epoch).
      return;
    }
    p.active.add(i, loc);
  }

  /** Parallel state, only exists for partitions that ever split or have multiple locations. */
  private static class ParallelState {
    // Active locations, sorted by epoch ascending. Includes soft-retired ones, which stay
    // writable until they hard-split.
    final CopyOnWriteArrayList<PartitionLocation> active = new CopyOnWriteArrayList<>();
    // epoch -> retire cause
    final ConcurrentHashMap<Integer, StatusCode> retired = new ConcurrentHashMap<>();
    volatile int maxEpoch = -1;
  }
}
