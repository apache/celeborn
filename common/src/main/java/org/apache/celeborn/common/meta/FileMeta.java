package org.apache.celeborn.common.meta;

import java.util.List;

import org.apache.commons.lang3.NotImplementedException;

public interface FileMeta {
  default List<Long> getChunkOffsets() {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }

  default void setBufferSize(int bufferSize) {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }

  default void setNumSubpartitions(int numSubpartitions) {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }

  default int getBufferSize() {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }

  default int getNumSubPartitions() {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }

  default void addChunkOffset(long offset) {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }

  default void setSorted() {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }

  default boolean getSorted() {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }

  default void setMountPoint(String mountPoint) {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }

  default String getMountPoint() {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }

  default int getNumSubpartitions() {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }

  default long getLastChunkOffset() {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }
}
