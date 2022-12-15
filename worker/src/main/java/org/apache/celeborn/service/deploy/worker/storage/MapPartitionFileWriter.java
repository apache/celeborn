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

package org.apache.celeborn.service.deploy.worker.storage;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.netty.buffer.CompositeByteBuf;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.protocol.PartitionSplitMode;
import org.apache.celeborn.common.protocol.PartitionType;

/*
 * map partition file writer, it will create index for each partition
 */
public final class MapPartitionFileWriter extends FileWriter {
  private static final Logger logger = LoggerFactory.getLogger(MapPartitionFileWriter.class);

  private int numReducePartitions;
  private int currentDataRegionIndex;
  private boolean isBroadcastRegion;
  private long[] numReducePartitionBytes;
  private ByteBuffer indexBuffer;
  private int currentReducePartition;
  private long totalBytes;
  private long regionStartingOffset;
  private long numDataRegions;
  private FileChannel channelIndex;
  private FSDataOutputStream streamIndex;
  private CompositeByteBuf flushBufferIndex;

  public MapPartitionFileWriter(
      FileInfo fileInfo,
      Flusher flusher,
      AbstractSource workerSource,
      CelebornConf conf,
      DeviceMonitor deviceMonitor,
      long splitThreshold,
      PartitionSplitMode splitMode,
      boolean rangeReadFilter)
      throws IOException {
    super(
        fileInfo,
        flusher,
        workerSource,
        conf,
        deviceMonitor,
        splitThreshold,
        splitMode,
        PartitionType.MAP,
        rangeReadFilter);
    if (!fileInfo.isHdfs()) {
      channelIndex = new FileOutputStream(fileInfo.getIndexPath()).getChannel();
    } else {
      streamIndex = StorageManager.hadoopFs().create(fileInfo.getHdfsIndexPath(), true);
    }
  }

  @Override
  public long close() throws IOException {
    return 0;
  }

  public void pushDataHandShake(int numReducePartitions) {
    this.numReducePartitions = numReducePartitions;
  }

  public void regionStart(int currentDataRegionIndex, boolean isBroadcastRegion) {
    this.currentDataRegionIndex = currentDataRegionIndex;
    this.isBroadcastRegion = isBroadcastRegion;
  }

  public void regionFinish() throws IOException {}
}
