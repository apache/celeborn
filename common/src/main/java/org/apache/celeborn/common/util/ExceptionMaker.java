package org.apache.celeborn.common.util;

public abstract class ExceptionMaker {
  private int appShuffleId;
  private int shuffleId;
  private int partitionId;
  private Exception e;

  public abstract Exception makeException(
      int appShuffleId, int shuffleId, int partitionId, Exception e);
}
