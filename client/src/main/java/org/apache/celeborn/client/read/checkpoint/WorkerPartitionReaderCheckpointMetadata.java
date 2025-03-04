package org.apache.celeborn.client.read.checkpoint;

import java.util.Set;

public class WorkerPartitionReaderCheckpointMetadata implements PartitionReaderCheckpointMetadata {
  private final Set<Integer> returnedChunks;

  public WorkerPartitionReaderCheckpointMetadata(Set<Integer> returnedChunks) {
    this.returnedChunks = returnedChunks;
  }

  public Set<Integer> getReturnedChunks() {
    return returnedChunks;
  }
}
