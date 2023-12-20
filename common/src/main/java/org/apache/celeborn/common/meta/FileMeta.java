/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  default int getNumChunks() {
    throw new NotImplementedException(
        this.getClass().getSimpleName() + " did not implement this method");
  }
}
