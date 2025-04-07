package org.apache.spark.shuffle.celeborn;

import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

public class CelebornIntegrityCheckPlugin implements SparkPlugin {
  @Override
  public DriverPlugin driverPlugin() {
    return null;
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    return new CelebornIntegrityCheckExecutorPlugin();
  }
}
