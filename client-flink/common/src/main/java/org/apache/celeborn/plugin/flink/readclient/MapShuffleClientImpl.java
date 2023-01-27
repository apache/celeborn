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

package org.apache.celeborn.plugin.flink.readclient;

import java.io.IOException;

import org.apache.celeborn.client.ShuffleClientImpl;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.plugin.flink.network.MapTransportClientFactory;
import org.apache.celeborn.plugin.flink.network.MapTransportContext;
import org.apache.celeborn.plugin.flink.network.ReadClientHandler;

public class MapShuffleClientImpl extends ShuffleClientImpl {
  private MapTransportClientFactory mapTransportClientFactory;
  private ReadClientHandler readClientHandler = new ReadClientHandler();

  public MapShuffleClientImpl(
      String driverHost, int port, CelebornConf conf, UserIdentifier userIdentifier) {
    super(conf, userIdentifier);
    String module = TransportModuleConstants.DATA_MODULE;
    TransportConf dataTransportConf =
        Utils.fromCelebornConf(conf, module, conf.getInt("celeborn" + module + ".io.threads", 8));
    MapTransportContext context =
        new MapTransportContext(
            dataTransportConf, readClientHandler, conf.clientCloseIdleConnections());
    this.mapTransportClientFactory = new MapTransportClientFactory(context);
    this.setupMetaServiceRef(driverHost, port);
  }

  public RssBufferStream readBufferedPartition(
      String applicationId,
      int shuffleId,
      int partitionId,
      int subPartitionIndexStart,
      int subPartitionIndexEnd)
      throws IOException {
    String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
    ReduceFileGroups fileGroups = loadFileGroup(applicationId, shuffleKey, shuffleId, partitionId);
    if (fileGroups.partitionGroups.size() == 0
        || !fileGroups.partitionGroups.containsKey(partitionId)) {
      logger.warn("Shuffle data is empty for shuffle {} partitionId {}.", shuffleId, partitionId);
      return RssBufferStream.empty();
    } else {
      return RssBufferStream.create(
          conf,
          mapTransportClientFactory,
          shuffleKey,
          fileGroups.partitionGroups.get(partitionId).toArray(new PartitionLocation[0]),
          subPartitionIndexStart,
          subPartitionIndexEnd);
    }
  }

  public ReadClientHandler getReadClientHandler() {
    return readClientHandler;
  }
}
