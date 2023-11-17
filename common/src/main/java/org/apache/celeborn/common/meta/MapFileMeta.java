package org.apache.celeborn.common.meta;

import java.util.List;

import org.apache.commons.lang3.NotImplementedException;

public class MapFileMeta implements FileMeta {
  int bufferSize;
  int numSubPartitions;
  String mountPoint;

  public MapFileMeta(int bufferSize, int numSubPartitions) {
    this.bufferSize = bufferSize;
    this.numSubPartitions = numSubPartitions;
  }

  @Override
  public List<Long> getChunkOffsets() {
    throw new NotImplementedException("Map file meta did not implement this method");
  }

  @Override
  public int getBufferSize() {
    return bufferSize;
  }

  @Override
  public int getNumSubPartitions() {
    return numSubPartitions;
  }

  @Override
  public void addChunkOffset(long offset) {
    throw new NotImplementedException("Map file meta did not implement this method");
  }

  @Override
  public void setSorted() {
    throw new NotImplementedException("Map file meta did not implement this method");
  }

  @Override
  public boolean getSorted() {
    throw new NotImplementedException("Map file meta did not implement this method");
  }

  @Override
  public void setMountPoint(String mountPoint) {
    this.mountPoint = mountPoint;
  }

  @Override
  public String getMountPoint() {
    return mountPoint;
  }

  @Override
  public int getNumSubpartitions() {
    return numSubPartitions;
  }
}
