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

package org.apache.celeborn.common.protocol;

import java.io.Serializable;

import org.roaringbitmap.RoaringBitmap;

/** Snapshot of the pre-optimization layout, used only as the JOL comparison baseline. */
final class PartitionLocationOld implements Serializable {
  private int id;
  private int epoch;
  private String host;
  private int rpcPort;
  private int pushPort;
  private int fetchPort;
  private int replicatePort;
  private PartitionLocation.Mode mode;
  private PartitionLocationOld peer;
  private StorageInfo storageInfo;
  private RoaringBitmap mapIdBitMap;
  private transient String _hostPushPort;
  private transient String _hostFetchPort;

  PartitionLocationOld(
      int id,
      int epoch,
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      PartitionLocation.Mode mode,
      PartitionLocationOld peer,
      StorageInfo storageInfo,
      RoaringBitmap mapIdBitMap) {
    this.id = id;
    this.epoch = epoch;
    this.host = host;
    this.rpcPort = rpcPort;
    this.pushPort = pushPort;
    this.fetchPort = fetchPort;
    this.replicatePort = replicatePort;
    this.mode = mode;
    this.peer = peer;
    this.storageInfo = storageInfo;
    this.mapIdBitMap = mapIdBitMap;
  }

  void setPeer(PartitionLocationOld peer) {
    this.peer = peer;
  }
}
