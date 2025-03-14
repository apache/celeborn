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

package org.apache.celeborn.client.read.checkpoint;

import java.util.HashSet;
import java.util.Set;

/** Checkpoint metadata interface for a partition reader. */
public class PartitionReaderCheckpointMetadata {
  private final Set<Integer> returnedChunks;

  /** Create an instance of the checkpoint metadata. */
  public PartitionReaderCheckpointMetadata() {
    this.returnedChunks = new HashSet<>();
  }

  public void checkpoint(int chunkId) {
    returnedChunks.add(chunkId);
  }

  public boolean isCheckpointed(int chunkId) {
    return returnedChunks.contains(chunkId);
  }

  public Set<Integer> getReturnedChunks() {
    return returnedChunks;
  }
}
