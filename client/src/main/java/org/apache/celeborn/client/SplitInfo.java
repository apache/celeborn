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

package org.apache.celeborn.client;

public class SplitInfo {
  private int partitionId;
  private int epoch;
  private int splitStart;
  private int splitEnd;

  public SplitInfo(int partitionId, int epoch, int splitStart, int splitEnd) {
    this.partitionId = partitionId;
    this.epoch = epoch;
    this.splitStart = splitStart;
    this.splitEnd = splitEnd;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public int getEpoch() {
    return epoch;
  }

  public int getSplitStart() {
    return splitStart;
  }

  public int getSplitEnd() {
    return splitEnd;
  }
}
