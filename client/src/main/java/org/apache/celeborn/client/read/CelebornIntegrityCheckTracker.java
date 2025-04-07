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

public class CelebornIntegrityCheckTracker {
  private static final Logger logger = LoggerFactory.getLogger(CelebornIntegrityCheckTracker.class);
  private static final ThreadLocal<CelebornReaderContext> readerContext = new ThreadLocal<>();

  private static Supplier<CelebornReaderContext> contextCreator = CelebornReaderContext::new;

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
      return "CelebornIntegrityCheckTracker{"
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
      var removed = readers.remove(reader);
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
