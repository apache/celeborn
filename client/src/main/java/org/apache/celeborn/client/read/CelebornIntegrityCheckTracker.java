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

package org.apache.celeborn.client.read;

import static com.google.common.base.Preconditions.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-local registry that enforces data integrity checks for every CelebornInputStream created
 * within a Spark task.
 *
 * <p>Lifecycle (driven by {@code CelebornIntegrityCheckExecutorPlugin}):
 *
 * <ol>
 *   <li>{@link #startTask()} — called at task start, initialises the per-task context.
 *   <li>{@link #registerReader(Iterator)} — called when a {@code ValidatingIterator} is
 *       constructed; records it so we can later verify it was fully consumed.
 *   <li>{@link #registerValidation(int, int, int, int)} — called from {@link CelebornInputStream}'s
 *       {@code validateIntegrity()} after the integrity RPC succeeds.
 *   <li>{@link #ensureIntegrityCheck} — called by {@code ValidatingIterator} when {@code hasNext()}
 *       first returns {@code false}; asserts that every expected partition has a matching {@code
 *       registerValidation} entry.
 *   <li>{@link #finishTask()} — called on task success; asserts no readers finished prematurely,
 *       then cleans up.
 *   <li>{@link #discard()} — called on task failure; skips assertions and cleans up.
 * </ol>
 */
public class CelebornIntegrityCheckTracker {
  private static final Logger logger = LoggerFactory.getLogger(CelebornIntegrityCheckTracker.class);
  private static final ThreadLocal<CelebornReaderContext> readerContext = createReaderContext();

  private static Supplier<CelebornReaderContext> contextCreator = CelebornReaderContext::new;

  public static ThreadLocal<CelebornReaderContext> createReaderContext() {
    try {
      String useInheritable = System.getenv("CELEBORN_INTEGRITY_INHERITABLE");
      if (Boolean.parseBoolean(useInheritable)) {
        return new InheritableThreadLocal<>();
      }
    } catch (Exception e) {
      logger.error("Failed to check env variable CELEBORN_INTEGRITY_INHERITABLE", e);
    }
    return new ThreadLocal<>();
  }

  public static void ensureIntegrityCheck(
      scala.collection.Iterator<?> reader,
      int shuffleId,
      int startMapper,
      int endMapper,
      int minPartition,
      int maxPartition) {
    logger.info(
        "Validating {} shuffleId={} startMapper={} endMapper={} minPartition={} maxPartition={}",
        reader,
        shuffleId,
        startMapper,
        endMapper,
        minPartition,
        maxPartition);
    getCelebornReaderContext()
        .ensureIntegrityCheck(
            reader, shuffleId, startMapper, endMapper, minPartition, maxPartition);
  }

  @VisibleForTesting
  public static void setContextCreator(Supplier<CelebornReaderContext> contextCreator) {
    CelebornIntegrityCheckTracker.contextCreator = contextCreator;
  }

  private static CelebornReaderContext getCelebornReaderContext() {
    CelebornReaderContext celebornReaderContext = readerContext.get();
    checkNotNull(celebornReaderContext, "celebornReaderContext");
    return celebornReaderContext;
  }

  public static void registerValidation(
      int shuffleId, int startMapper, int endMapper, int partition) {
    getCelebornReaderContext().registerValidation(shuffleId, startMapper, endMapper, partition);
  }

  public static void checkEnabled() {
    checkNotNull(readerContext.get(), "readerContext");
  }

  public static void registerReader(scala.collection.Iterator<?> reader) {
    logger.info("Registering {}", reader);
    getCelebornReaderContext().registerReader(reader);
  }

  public static void startTask() {
    checkState(
        readerContext.get() == null, "Integrity check is already started: %s", readerContext.get());
    readerContext.set(contextCreator.get());
  }

  public static void finishTask() {
    getCelebornReaderContext().validateEmpty();
    discard();
  }

  public static void discard() {
    checkNotNull(readerContext.get(), "Integrity check not started");
    readerContext.remove();
  }

  private static class CelebornReaderContextPartition {
    private final int startMapper;
    private final int endMapper;
    private final int partition;
    private final int shuffleId;

    CelebornReaderContextPartition(int shuffleId, int startMapper, int endMapper, int partition) {
      this.shuffleId = shuffleId;
      this.startMapper = startMapper;
      this.endMapper = endMapper;
      this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      CelebornReaderContextPartition that = (CelebornReaderContextPartition) o;
      return shuffleId == that.shuffleId
          && startMapper == that.startMapper
          && endMapper == that.endMapper
          && partition == that.partition;
    }

    @Override
    public int hashCode() {
      return Objects.hash(shuffleId, startMapper, endMapper, partition);
    }

    @Override
    public String toString() {
      return "CelebornReaderContextPartition{"
          + "shuffleId="
          + shuffleId
          + ", startMapper="
          + startMapper
          + ", endMapper="
          + endMapper
          + ", partition="
          + partition
          + '}';
    }
  }

  @VisibleForTesting
  public static class CelebornReaderContext {
    private final HashMap<CelebornReaderContextPartition, Integer> performedChecks =
        new HashMap<>();
    private final Set<Iterator<?>> readers = new HashSet<>();

    public void registerValidation(int shuffleId, int startMapper, int endMapper, int partition) {
      performedChecks.merge(
          new CelebornReaderContextPartition(shuffleId, startMapper, endMapper, partition),
          1,
          Integer::sum);
    }

    public void ensureIntegrityCheck(
        Iterator<?> reader,
        int shuffleId,
        int startMapper,
        int endMapper,
        int minPartition,
        int maxPartition) {
      boolean removed = readers.remove(reader);
      checkState(removed, "Reader not registered: %s", reader);
      for (int partition = minPartition; partition < maxPartition; partition++) {
        CelebornReaderContextPartition key =
            new CelebornReaderContextPartition(shuffleId, startMapper, endMapper, partition);
        int res = performedChecks.merge(key, -1, Integer::sum);
        checkArgument(
            res >= 0,
            "Validation is not registered %s (%s). Registered validations: %s",
            key,
            res,
            performedChecks);
        if (res == 0) {
          performedChecks.remove(key);
        }
      }
    }

    @Override
    public String toString() {
      return "CelebornReaderContext{" + "performedChecks=" + performedChecks + '}';
    }

    public void validateEmpty() {
      if (!readers.isEmpty()) {
        logger.error(
            "Some readers finished prematurely, it means spark didn't read all the records: {}",
            readers);
      }
    }

    public <T> void registerReader(Iterator<T> reader) {
      readers.add(reader);
    }
  }
}
