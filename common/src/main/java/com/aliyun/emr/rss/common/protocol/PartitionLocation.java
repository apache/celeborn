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

package com.aliyun.emr.rss.common.protocol;

import java.io.Serializable;

import org.roaringbitmap.RoaringBitmap;

import com.aliyun.emr.rss.common.meta.WorkerInfo;
import com.aliyun.emr.rss.common.util.Utils;

public class PartitionLocation implements Serializable {
  public enum Mode {
    Master(0), Slave(1);

    private final byte mode;

    Mode(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.mode = (byte) id;
    }

    public byte mode() { return mode; }
  }

  public enum StorageHint {
    NON_EXISTS, MEMORY, HDD, SDD, HDFS, OSS
  }

  public enum Type {
    REDUCE_PARTITION, MAP_PARTITION, MAPGROUP_REDUCE_PARTITION
  }

  public static PartitionLocation.Mode getMode(byte mode) {
    if (mode == 0) {
      return Mode.Master;
    } else {
      return Mode.Slave;
    }
  }

  private int reduceId;
  private int epoch;
  private String host;
  private int rpcPort;
  private int pushPort;
  private int fetchPort;
  private int replicatePort;
  private Mode mode;
  private PartitionLocation peer;
  private StorageHint storageHint;
  private RoaringBitmap mapIdBitMap = null;

  public PartitionLocation(PartitionLocation loc) {
    this.reduceId = loc.reduceId;
    this.epoch = loc.epoch;
    this.host = loc.host;
    this.rpcPort = loc.rpcPort;
    this.pushPort = loc.pushPort;
    this.fetchPort = loc.fetchPort;
    this.replicatePort = loc.replicatePort;
    this.mode = loc.mode;
    this.peer = loc.peer;
    this.storageHint = loc.storageHint;
  }

  public PartitionLocation(
    int reduceId,
    int epoch,
    String host,
    int rpcPort,
    int pushPort,
    int fetchPort,
    int replicatePort,
    Mode mode) {
    this(reduceId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort,
      mode, null, StorageHint.MEMORY,null);
  }

  public PartitionLocation(
    int reduceId,
    int epoch,
    String host,
    int rpcPort,
    int pushPort,
    int fetchPort,
    int replicatePort,
    Mode mode,
    StorageHint storageHint) {
    this(reduceId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort,
      mode, null, storageHint,null);
  }

  public PartitionLocation(
    int reduceId,
    int epoch,
    String host,
    int rpcPort,
    int pushPort,
    int fetchPort,
    int replicatePort,
    Mode mode,
    PartitionLocation peer) {
    this(reduceId, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer,
      StorageHint.MEMORY,null);
  }

  public PartitionLocation(
    int reduceId,
    int epoch,
    String host,
    int rpcPort,
    int pushPort,
    int fetchPort,
    int replicatePort,
    Mode mode,
    PartitionLocation peer,
    StorageHint hint,
    RoaringBitmap mapIdBitMap) {
    this.reduceId = reduceId;
    this.epoch = epoch;
    this.host = host;
    this.rpcPort = rpcPort;
    this.pushPort = pushPort;
    this.fetchPort = fetchPort;
    this.replicatePort = replicatePort;
    this.mode = mode;
    this.peer = peer;
    this.storageHint = hint;
    this.mapIdBitMap = mapIdBitMap;
  }

  public int getReduceId()
  {
    return reduceId;
  }

  public void setReduceId(int reduceId)
  {
    this.reduceId = reduceId;
  }

  public int getEpoch() {
    return epoch;
  }

  public void setEpoch(int epoch) {
    this.epoch = epoch;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPushPort() {
    return pushPort;
  }

  public void setPushPort(int pushPort) {
    this.pushPort = pushPort;
  }

  public int getFetchPort() {
    return fetchPort;
  }

  public void setFetchPort(int fetchPort) {
    this.fetchPort = fetchPort;
  }

  public String hostAndPorts() {
    return host + ":" + rpcPort + ":" + pushPort + ":" + fetchPort;
  }

  public String hostAndPushPort() {
    return host+":"+pushPort;
  }

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public PartitionLocation getPeer() {
    return peer;
  }

  public void setPeer(PartitionLocation peer) {
    this.peer = peer;
  }

  public String getUniqueId() {
    return reduceId + "-" + epoch;
  }

  public String getFileName() {
    return reduceId + "-" + epoch + "-" + mode.mode;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public int getReplicatePort() {
    return replicatePort;
  }

  public void setReplicatePort(int replicatePort) {
    this.replicatePort = replicatePort;
  }

  public StorageHint getStorageHint() {
    return storageHint;
  }

  public void setStorageHint(StorageHint storageHint) {
    this.storageHint = storageHint;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PartitionLocation)) {
      return false;
    }
    PartitionLocation o = (PartitionLocation) other;
    return reduceId == o.reduceId
        && epoch == o.epoch
        && host.equals(o.host)
        && rpcPort == o.rpcPort
        && pushPort == o.pushPort
        && fetchPort == o.fetchPort;
  }

  @Override
  public int hashCode() {
    return (reduceId + epoch + host + rpcPort + pushPort + fetchPort).hashCode();
  }

  @Override
  public String toString() {
    String peerAddr = "";
    if (peer != null) {
      peerAddr = peer.hostAndPorts();
    }
    return "PartitionLocation[" + reduceId + "-" + epoch + " " + host + ":" + rpcPort + ":" +
             pushPort + ":" + fetchPort + ":" + replicatePort + " Mode: " + mode +
             " peer: " + peerAddr + "storage hint:" + storageHint + "]";
  }

  public WorkerInfo getWorker() {
    return new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort);
  }

  public RoaringBitmap getMapIdBitMap() {
    return mapIdBitMap;
  }

  public void setMapIdBitMap(RoaringBitmap mapIdBitMap) {
    this.mapIdBitMap = mapIdBitMap;
  }

  public static PartitionLocation fromPbPartitionLocation(TransportMessages.PbPartitionLocation
                                                            pbPartitionLocation) {
    Mode mode = Mode.Master;
    if (pbPartitionLocation.getMode() == TransportMessages.PbPartitionLocation.Mode.Slave) {
      mode = Mode.Slave;
    }

    PartitionLocation partitionLocation = new PartitionLocation(
        pbPartitionLocation.getReduceId(),
        pbPartitionLocation.getEpoch(),
        pbPartitionLocation.getHost(),
        pbPartitionLocation.getRpcPort(),
        pbPartitionLocation.getPushPort(),
        pbPartitionLocation.getFetchPort(),
        pbPartitionLocation.getReplicatePort(),
        mode,
        null,
        PartitionLocation.StorageHint.values()[pbPartitionLocation.getStorageHintOrdinal()],
        Utils.byteStringToRoaringBitmap(pbPartitionLocation.getMapIdBitmap()));

    if (pbPartitionLocation.hasPeer()) {
      TransportMessages.PbPartitionLocation peerPb = pbPartitionLocation.getPeer();
      Mode peerMode = Mode.Master;
      if (peerPb.getMode() == TransportMessages.PbPartitionLocation.Mode.Slave) {
        peerMode = Mode.Slave;
      }
      PartitionLocation peerLocation = new PartitionLocation(peerPb.getReduceId(),
        peerPb.getEpoch(), peerPb.getHost(), peerPb.getRpcPort(), peerPb.getPushPort(),
        peerPb.getFetchPort(), peerPb.getReplicatePort(), peerMode, partitionLocation,
        PartitionLocation.StorageHint.values()[peerPb.getStorageHintOrdinal()],
          Utils.byteStringToRoaringBitmap(peerPb.getMapIdBitmap()));
      partitionLocation.setPeer(peerLocation);
    }

    return partitionLocation;
  }

  public static TransportMessages.PbPartitionLocation toPbPartitionLocation(PartitionLocation
                                                                              partitionLocation) {
    TransportMessages.PbPartitionLocation.Builder pbPartitionLocationBuilder = TransportMessages
      .PbPartitionLocation.newBuilder();
    if (partitionLocation.mode == Mode.Master) {
      pbPartitionLocationBuilder.setMode(TransportMessages.PbPartitionLocation.Mode.Master);
    } else {
      pbPartitionLocationBuilder.setMode(TransportMessages.PbPartitionLocation.Mode.Slave);
    }
    pbPartitionLocationBuilder.setHost(partitionLocation.getHost());
    pbPartitionLocationBuilder.setEpoch(partitionLocation.getEpoch());
    pbPartitionLocationBuilder.setReduceId(partitionLocation.getReduceId());
    pbPartitionLocationBuilder.setRpcPort(partitionLocation.getRpcPort());
    pbPartitionLocationBuilder.setPushPort(partitionLocation.getPushPort());
    pbPartitionLocationBuilder.setFetchPort(partitionLocation.getFetchPort());
    pbPartitionLocationBuilder.setReplicatePort(partitionLocation.getReplicatePort());
    pbPartitionLocationBuilder.setStorageHintOrdinal(partitionLocation.getStorageHint().ordinal());
    pbPartitionLocationBuilder.setMapIdBitmap(
        Utils.roaringBitmapToByteString(partitionLocation.getMapIdBitMap()));

    if (partitionLocation.getPeer() != null) {
      TransportMessages.PbPartitionLocation.Builder peerPbPartionLocationBuilder = TransportMessages
        .PbPartitionLocation.newBuilder();
      if (partitionLocation.getPeer().mode == Mode.Master) {
        peerPbPartionLocationBuilder.setMode(TransportMessages.PbPartitionLocation.Mode.Master);
      } else {
        peerPbPartionLocationBuilder.setMode(TransportMessages.PbPartitionLocation.Mode.Slave);
      }
      peerPbPartionLocationBuilder.setHost(partitionLocation.getPeer().getHost());
      peerPbPartionLocationBuilder.setEpoch(partitionLocation.getPeer().getEpoch());
      peerPbPartionLocationBuilder.setReduceId(partitionLocation.getPeer().getReduceId());
      peerPbPartionLocationBuilder.setRpcPort(partitionLocation.getPeer().getRpcPort());
      peerPbPartionLocationBuilder.setPushPort(partitionLocation.getPeer().getPushPort());
      peerPbPartionLocationBuilder.setFetchPort(partitionLocation.getPeer().getFetchPort());
      peerPbPartionLocationBuilder.setReplicatePort(partitionLocation.getPeer().getReplicatePort());
      peerPbPartionLocationBuilder.setStorageHintOrdinal(
        partitionLocation.getPeer().getStorageHint().ordinal());
      peerPbPartionLocationBuilder.setMapIdBitmap(Utils.roaringBitmapToByteString(
          partitionLocation.getPeer().getMapIdBitMap()));
      pbPartitionLocationBuilder.setPeer(peerPbPartionLocationBuilder.build());
    }

    return pbPartitionLocationBuilder.build();
  }
}
