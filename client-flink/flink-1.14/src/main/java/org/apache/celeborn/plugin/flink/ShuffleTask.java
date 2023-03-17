package org.apache.celeborn.plugin.flink;

public class ShuffleTask {
  public final int shuffleId;
  public final int mapId;
  public final int attemptId;

  public ShuffleTask(int shuffleId, int mapId, int attemptId) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptId = attemptId;
  }
}
