package org.apache.celeborn.common.util;

public interface ExceptionMaker {
  Exception makeException(int appShuffleId, int shuffleId, int partitionId, Exception e);
}
