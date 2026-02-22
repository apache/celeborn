package org.apache.celeborn.tests.spark.s3;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle;
import org.apache.spark.shuffle.celeborn.SparkShuffleManager;

public class ShuffleManagerSpy extends SparkShuffleManager {

  private static AtomicReference<Callback<?, ?, ?>> getShuffleReaderHook = new AtomicReference<>();

  public ShuffleManagerSpy(SparkConf conf, boolean isDriver) {
    super(conf, isDriver);
  }

  @Override
  public <K, C> ShuffleReader<K, C> getCelebornShuffleReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      int startMapIndex,
      int endMapIndex,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    Callback consumer = getShuffleReaderHook.get();
    if (consumer != null) {
      CelebornShuffleHandle celebornShuffleHandle = (CelebornShuffleHandle) handle;
      consumer.accept(celebornShuffleHandle, startPartition, endPartition);
    }
    return super.getCelebornShuffleReader(
        handle, startPartition, endPartition, startMapIndex, endMapIndex, context, metrics);
  }

  interface Callback<K, V, T> {
    void accept(
        CelebornShuffleHandle<K, V, T> handle, Integer startPartition, Integer endPartition);
  }

  public static <K, V, T> void interceptOpenShuffleReader(Callback<K, V, T> hook) {
    getShuffleReaderHook.set(hook);
  }

  public static void resetHook() {
    getShuffleReaderHook.set(null);
  }
}
