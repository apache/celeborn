package org.apache.celeborn.client;

public class SplitInfo {
  private int partitionId;
  private int epoch;
  private int splitStart;
  private int splitEnd;

  public SplitInfo(int partitionId, int epoch, int splitStart, int splitEnd) {
    this.partitionId = partitionId;
    this.epoch = epoch;
    this.splitStart = splitStart;
    this.splitEnd = splitEnd;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public int getEpoch() {
    return epoch;
  }

  public int getSplitStart() {
    return splitStart;
  }

  public int getSplitEnd() {
    return splitEnd;
  }
}
