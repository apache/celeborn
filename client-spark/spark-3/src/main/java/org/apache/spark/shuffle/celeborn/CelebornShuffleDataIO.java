package org.apache.spark.shuffle.celeborn;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDriverComponents;
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;

public class CelebornShuffleDataIO implements ShuffleDataIO {

  private static final Logger logger = LoggerFactory.getLogger(CelebornShuffleDataIO.class);

  private final SparkConf sparkConf;
  private final CelebornConf celebornConf;

  public CelebornShuffleDataIO(SparkConf sparkConf) {
    logger.info("Loading CelebornShuffleDataIO");
    this.sparkConf = sparkConf;
    this.celebornConf = SparkUtils.fromSparkConf(sparkConf);
  }

  @Override
  public ShuffleExecutorComponents executor() {
    // Used when fallback to Spark SortShuffleManager
    return new LocalDiskShuffleExecutorComponents(sparkConf);
  }

  @Override
  public ShuffleDriverComponents driver() {
    return new CelebornShuffleDriverComponents(celebornConf);
  }
}

class CelebornShuffleDriverComponents extends LocalDiskShuffleDriverComponents {

  private final boolean support;

  public CelebornShuffleDriverComponents(CelebornConf celebornConf) {
    this.support = !celebornConf.shuffleForceFallbackEnabled();
  }

  // Omitting @Override annotation to avoid compile error before Spark 3.5.0
  public boolean supportsReliableStorage() {
    return support;
  }
}
