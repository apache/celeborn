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

import org.apache.celeborn.common.meta.WorkerInfo;
import org.apache.celeborn.common.util.Utils;

public class PartitionLocation implements Serializable {
  public enum Mode {
    MASTER(0),
    SLAVE(1);

    private final byte mode;

    Mode(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.mode = (byte) id;
    }

    public byte mode() {
      return mode;
    }
  }

  public static PartitionLocation.Mode getMode(byte mode) {
    if (mode == 0) {
      return Mode.MASTER;
    } else {
      return Mode.SLAVE;
    }
  }

  private int id;
  private int epoch;
  private String host;
  private int rpcPort;
  private int pushPort;
  private int fetchPort;
  private int replicatePort;
  private Mode mode;
  private PartitionLocation peer;
  private StorageInfo storageInfo = new StorageInfo();
  private RoaringBitmap mapIdBitMap = null;

  public PartitionLocation(PartitionLocation loc) {
    this.id = loc.id;
    this.epoch = loc.epoch;
    this.host = loc.host;
    this.rpcPort = loc.rpcPort;
    this.pushPort = loc.pushPort;
    this.fetchPort = loc.fetchPort;
    this.replicatePort = loc.replicatePort;
    this.mode = loc.mode;
    this.peer = loc.peer;
    this.storageInfo = loc.storageInfo;
  }

  public PartitionLocation(
      int id,
      int epoch,
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Mode mode) {
    this(
        id,
        epoch,
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        mode,
        null,
        new StorageInfo(),
        new RoaringBitmap());
  }

  public PartitionLocation(
      int id,
      int epoch,
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Mode mode,
      PartitionLocation peer) {
    this(
        id,
        epoch,
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        mode,
        peer,
        new StorageInfo(),
        new RoaringBitmap());
  }

  public PartitionLocation(
      int id,
      int epoch,
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Mode mode,
      PartitionLocation peer,
      StorageInfo hint,
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
    this.storageInfo = hint;
    this.mapIdBitMap = mapIdBitMap;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
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
    return host + ":" + pushPort;
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
    return id + "-" + epoch;
  }

  public String getFileName() {
    return id + "-" + epoch + "-" + mode.mode;
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

  public StorageInfo getStorageInfo() {
    return storageInfo;
  }

  public void setStorageInfo(StorageInfo storageInfo) {
    this.storageInfo = storageInfo;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PartitionLocation)) {
      return false;
    }
    PartitionLocation o = (PartitionLocation) other;
    return id == o.id
        && epoch == o.epoch
        && host.equals(o.host)
        && rpcPort == o.rpcPort
        && pushPort == o.pushPort
        && fetchPort == o.fetchPort;
  }

  @Override
  public int hashCode() {
    return (id + epoch + host + rpcPort + pushPort + fetchPort).hashCode();
  }

  @Override
  public String toString() {
    String peerAddr = "";
    if (peer != null) {
      peerAddr = peer.hostAndPorts();
    }
    return "PartitionLocation["
        + id
        + "-"
        + epoch
        + " "
        + host
        + ":"
        + rpcPort
        + ":"
        + pushPort
        + ":"
        + fetchPort
        + ":"
        + replicatePort
        + " Mode: "
        + mode
        + " peer: "
        + peerAddr
        + " storage hint:"
        + storageInfo
        + "]";
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

  public static PartitionLocation fromPbPartitionLocation(PbPartitionLocation pbLoc) {
    Mode mode = Mode.MASTER;
    if (pbLoc.getMode() == PbPartitionLocation.Mode.Slave) {
      mode = Mode.SLAVE;
    }

    PartitionLocation partitionLocation =
        new PartitionLocation(
            pbLoc.getId(),
            pbLoc.getEpoch(),
            pbLoc.getHost(),
            pbLoc.getRpcPort(),
            pbLoc.getPushPort(),
            pbLoc.getFetchPort(),
            pbLoc.getReplicatePort(),
            mode,
            null,
            StorageInfo.fromPb(pbLoc.getStorageInfo()),
            Utils.byteStringToRoaringBitmap(pbLoc.getMapIdBitmap()));
    if (pbLoc.hasPeer()) {
      PbPartitionLocation peerPb = pbLoc.getPeer();
      Mode peerMode = Mode.MASTER;
      if (peerPb.getMode() == PbPartitionLocation.Mode.Slave) {
        peerMode = Mode.SLAVE;
      }
      PartitionLocation peerLocation =
          new PartitionLocation(
              peerPb.getId(),
              peerPb.getEpoch(),
              peerPb.getHost(),
              peerPb.getRpcPort(),
              peerPb.getPushPort(),
              peerPb.getFetchPort(),
              peerPb.getReplicatePort(),
              peerMode,
              partitionLocation,
              StorageInfo.fromPb(peerPb.getStorageInfo()),
              Utils.byteStringToRoaringBitmap(peerPb.getMapIdBitmap()));
      partitionLocation.setPeer(peerLocation);
    }

    return partitionLocation;
  }

  public static PbPartitionLocation toPbPartitionLocation(PartitionLocation location) {
    PbPartitionLocation.Builder builder = PbPartitionLocation.newBuilder();
    if (location.mode == Mode.MASTER) {
      builder.setMode(PbPartitionLocation.Mode.Master);
    } else {
      builder.setMode(PbPartitionLocation.Mode.Slave);
    }
    builder.setHost(location.getHost());
    builder.setEpoch(location.getEpoch());
    builder.setId(location.getId());
    builder.setRpcPort(location.getRpcPort());
    builder.setPushPort(location.getPushPort());
    builder.setFetchPort(location.getFetchPort());
    builder.setReplicatePort(location.getReplicatePort());
    builder.setStorageInfo(StorageInfo.toPb(location.storageInfo));
    builder.setMapIdBitmap(Utils.roaringBitmapToByteString(location.getMapIdBitMap()));

    if (location.getPeer() != null) {
      PbPartitionLocation.Builder peerBuilder = PbPartitionLocation.newBuilder();
      if (location.getPeer().mode == Mode.MASTER) {
        peerBuilder.setMode(PbPartitionLocation.Mode.Master);
      } else {
        peerBuilder.setMode(PbPartitionLocation.Mode.Slave);
      }
      peerBuilder.setHost(location.getPeer().getHost());
      peerBuilder.setEpoch(location.getPeer().getEpoch());
      peerBuilder.setId(location.getPeer().getId());
      peerBuilder.setRpcPort(location.getPeer().getRpcPort());
      peerBuilder.setPushPort(location.getPeer().getPushPort());
      peerBuilder.setFetchPort(location.getPeer().getFetchPort());
      peerBuilder.setReplicatePort(location.getPeer().getReplicatePort());
      peerBuilder.setStorageInfo(StorageInfo.toPb(location.getPeer().getStorageInfo()));
      peerBuilder.setMapIdBitmap(Utils.roaringBitmapToByteString(location.getMapIdBitMap()));
      builder.setPeer(peerBuilder.build());
    }

    return builder.build();
  }
}
