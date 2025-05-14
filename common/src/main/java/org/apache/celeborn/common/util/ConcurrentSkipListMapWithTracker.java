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

package org.apache.celeborn.common.util;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrentSkipListMapWithTracker<K, V> {
  private final ConcurrentSkipListMap<K, V> map = new ConcurrentSkipListMap<>();
  private final long firstTime = System.nanoTime();
  private long lastTime = firstTime;
  private int maxCount = 1;
  private long totalCountAndTime = 0;

  public V put(K key, V value) {
    return map.computeIfAbsent(
        key,
        (v) -> {
          recordChange();
          return value;
        });
  }

  public boolean remove(K key) {
    AtomicBoolean exists = new AtomicBoolean(false);
    map.computeIfPresent(
        key,
        (k, v) -> {
          recordChange();
          exists.set(true);
          return null;
        });
    return exists.get();
  }

  public ConcurrentNavigableMap<K, V> tailMap(K key) {
    return map.tailMap(key);
  }

  public int size() {
    return map.size();
  }

  private void recordChange() {
    int currentCount = map.size();
    if (currentCount > maxCount) {
      maxCount = currentCount;
    }
    long currentTime = System.nanoTime();
    long duration = currentTime - lastTime;
    lastTime = currentTime;
    totalCountAndTime += currentCount * TimeUnit.NANOSECONDS.toMillis(duration);
  }

  public String report() {
    recordChange();
    double averageSize =
        (double) totalCountAndTime / TimeUnit.NANOSECONDS.toMillis(lastTime - firstTime);
    return String.format(
        "maxActiveLocationsCount: %d, avgActiveLocationsCount: %.2f", maxCount, averageSize);
  }
}
