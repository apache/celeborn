package org.apache.spark.shuffle.celeborn;

import static org.apache.celeborn.client.read.CelebornIntegrityCheckTracker.*;

import org.apache.spark.TaskFailedReason;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CelebornIntegrityCheckExecutorPlugin implements ExecutorPlugin {
  private static final Logger logger =
      LoggerFactory.getLogger(CelebornIntegrityCheckExecutorPlugin.class);

  @Override
  public void onTaskStart() {
    startTask();
  }

  @Override
  public void onTaskSucceeded() {
    finishTask();
  }

  @Override
  public void onTaskFailed(TaskFailedReason failureReason) {
    logger.info("Ignoring integrity check validation as task is failed");
    discard();
  }
}
