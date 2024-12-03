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

package org.apache.tez.runtime.library.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;

public class CelebornTezPerPartitionRecord extends TezSpillRecord {
  private int numPartitions;
  private int[] numRecordsPerPartition;

  public CelebornTezPerPartitionRecord(int numPartitions) {
    super(numPartitions);
    this.numPartitions = numPartitions;
  }

  public CelebornTezPerPartitionRecord(int numPartitions, int[] numRecordsPerPartition) {
    super(numPartitions);
    this.numPartitions = numPartitions;
    this.numRecordsPerPartition = numRecordsPerPartition;
  }

  public CelebornTezPerPartitionRecord(Path indexFileName, Configuration job) throws IOException {
    super(indexFileName, job);
  }

  @Override
  public int size() {
    return numPartitions;
  }

  @Override
  public CelebornTezIndexRecord getIndex(int i) {
    int records = numRecordsPerPartition[i];
    CelebornTezIndexRecord celebornTezIndexRecord = new CelebornTezIndexRecord();
    celebornTezIndexRecord.setData(!(records == 0));
    return celebornTezIndexRecord;
  }

  static class CelebornTezIndexRecord extends TezIndexRecord {
    private boolean hasData;

    private void setData(boolean hasData) {
      this.hasData = hasData;
    }

    @Override
    public boolean hasData() {
      return hasData;
    }
  }
}
