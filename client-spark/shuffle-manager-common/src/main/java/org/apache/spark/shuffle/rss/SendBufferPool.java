package org.apache.spark.shuffle.rss;

import java.util.LinkedList;

public class SendBufferPool {
  private static volatile SendBufferPool _instance;
  public static SendBufferPool get(int capacity) {
    if (_instance == null) {
      synchronized (SendBufferPool.class) {
        if (_instance == null) {
          _instance = new SendBufferPool(capacity);
        }
      }
    }
    return _instance;
  }

  private final int capacity;

  // numPartitions -> buffers
  private LinkedList<byte[][]> buffers;

  public SendBufferPool(int capacity) {
    this.capacity = capacity;
    buffers = new LinkedList<>();
  }

  public synchronized byte[][] aquireBuffer(int numPartitions) {
    for (int i = 0; i < buffers.size(); i++) {
      if (buffers.get(i).length == numPartitions) {
        return buffers.remove(i);
      }
    }
    if (buffers.size() == capacity) {
      buffers.removeFirst();
    }
    return null;
  }

  public synchronized void returnBuffer(byte[][] buffer) {
    if (buffers.size() == capacity) {
      buffers.removeFirst();
    }
    buffers.addLast(buffer);
  }
}
