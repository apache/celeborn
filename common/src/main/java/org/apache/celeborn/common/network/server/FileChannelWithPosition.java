package org.apache.celeborn.common.network.server;

import java.nio.channels.FileChannel;

public class FileChannelWithPosition {
  public FileChannel channel;
  public long position;

  public FileChannelWithPosition(FileChannel channel) {
    this.channel = channel;
    this.position = 0;
  }
}
