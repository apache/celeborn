package org.apache.celeborn.client.read;

import org.apache.celeborn.common.protocol.PartitionLocation;

public class ChunkClientException extends Throwable {
  private PartitionLocation location;

  public ChunkClientException(String message, Throwable cause, PartitionLocation location) {
    super(message, cause);
    this.location = location;
  }

  public PartitionLocation getLocation() {
    return location;
  }
}
