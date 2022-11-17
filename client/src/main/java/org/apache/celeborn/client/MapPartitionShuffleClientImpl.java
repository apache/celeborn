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

import java.io.IOException;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.protocol.PartitionLocation;

public class MapPartitionShuffleClientImpl extends ShuffleClientImpl {
  private static final Logger logger = LoggerFactory.getLogger(MapPartitionShuffleClientImpl.class);

  public MapPartitionShuffleClientImpl(CelebornConf conf, UserIdentifier userIdentifier) {
    super(conf, userIdentifier);
  }

  @Override
  public int pushDataMappartition(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      ByteBuf data,
      PartitionLocation location,
      BooleanSupplier closeCallBack)
      throws IOException {

    return 0;
  }

  @Override
  public Optional<PartitionLocation> regionStart(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      PartitionLocation location,
      int currentRegionIdx,
      boolean isBroadcast,
      long regionTotalSize)
      throws IOException {
    return Optional.empty();
  }

  @Override
  public void regionFinish(
      String applicationId, int shuffleId, int mapId, int attemptId, PartitionLocation location)
      throws IOException {}

  @Override
  public void pushDataHandShake(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int numPartitions,
      int bufferSize,
      PartitionLocation location)
      throws IOException {}
}
