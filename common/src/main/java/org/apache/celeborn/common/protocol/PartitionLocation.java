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

/**
 * Describes a partition location. Java serialization is retained for same-version internal use; its
 * serialized form is not a cross-version compatibility contract.
 */
public class PartitionLocation implements Serializable {
  private static final RoaringBitmap EMPTY_MAP_ID_BITMAP = new RoaringBitmap();

  public enum Mode {
    PRIMARY(0),
    REPLICA(1);

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
      return Mode.PRIMARY;
    } else {
      return Mode.REPLICA;
    }
  }

  public static String getFileName(String uniqueId, Mode mode) {
    return uniqueId + "-" + mode.mode();
  }

  private int id;
  private int epoch;
  private volatile WorkerEndpoint endpoint;
  private Mode mode;
  private PartitionLocation peer;
  private volatile StorageInfo storageInfo;
  private volatile RoaringBitmap mapIdBitMap;

  public PartitionLocation(PartitionLocation loc) {
    this.id = loc.id;
    this.epoch = loc.epoch;
    this.endpoint = loc.endpoint;
    this.mode = loc.mode;
    this.peer = loc.peer;
    this.storageInfo = loc.storageInfo;
    this.mapIdBitMap = loc.mapIdBitMap;
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
    this(id, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, null, null, null);
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
    this(id, epoch, host, rpcPort, pushPort, fetchPort, replicatePort, mode, peer, null, null);
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
    this.endpoint = WorkerEndpoint.apply(host, rpcPort, pushPort, fetchPort, replicatePort);
    this.mode = mode;
    setPeer(peer);
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
    return endpoint.host();
  }

  public synchronized void setHost(String host) {
    WorkerEndpoint current = endpoint;
    this.endpoint =
        WorkerEndpoint.apply(
            host,
            current.rpcPort(),
            current.pushPort(),
            current.fetchPort(),
            current.replicatePort());
  }

  public int getPushPort() {
    return endpoint.pushPort();
  }

  public synchronized void setPushPort(int pushPort) {
    WorkerEndpoint current = endpoint;
    this.endpoint =
        WorkerEndpoint.apply(
            current.host(),
            current.rpcPort(),
            pushPort,
            current.fetchPort(),
            current.replicatePort());
  }

  public int getFetchPort() {
    return endpoint.fetchPort();
  }

  public synchronized void setFetchPort(int fetchPort) {
    WorkerEndpoint current = endpoint;
    this.endpoint =
        WorkerEndpoint.apply(
            current.host(),
            current.rpcPort(),
            current.pushPort(),
            fetchPort,
            current.replicatePort());
  }

  public String hostAndPorts() {
    return "host-rpcPort-pushPort-fetchPort-replicatePort:"
        + getHost()
        + "-"
        + getRpcPort()
        + "-"
        + getPushPort()
        + "-"
        + getFetchPort()
        + "-"
        + getReplicatePort();
  }

  public String hostAndFetchPort() {
    return endpoint.hostAndFetchPort();
  }

  public String hostAndPushPort() {
    return endpoint.hostAndPushPort();
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

  public boolean hasPeer() {
    return peer != null;
  }

  public String getUniqueId() {
    return id + "-" + epoch;
  }

  /** @see PartitionLocation#getFileName */
  public String getFileName() {
    return id + "-" + epoch + "-" + mode.mode;
  }

  public int getRpcPort() {
    return endpoint.rpcPort();
  }

  public synchronized void setRpcPort(int rpcPort) {
    WorkerEndpoint current = endpoint;
    this.endpoint =
        WorkerEndpoint.apply(
            current.host(),
            rpcPort,
            current.pushPort(),
            current.fetchPort(),
            current.replicatePort());
  }

  public int getReplicatePort() {
    return endpoint.replicatePort();
  }

  public synchronized void setReplicatePort(int replicatePort) {
    WorkerEndpoint current = endpoint;
    this.endpoint =
        WorkerEndpoint.apply(
            current.host(),
            current.rpcPort(),
            current.pushPort(),
            current.fetchPort(),
            replicatePort);
  }

  public StorageInfo getStorageInfo() {
    return getStorageInfoOrCreate();
  }

  public StorageInfo getStorageInfoOrCreate() {
    StorageInfo current = storageInfo;
    if (current == null) {
      synchronized (this) {
        current = storageInfo;
        if (current == null) {
          current = new StorageInfo();
          storageInfo = current;
        }
      }
    }
    return current;
  }

  public synchronized void setStorageInfo(StorageInfo storageInfo) {
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
        && getHost().equals(o.getHost())
        && getRpcPort() == o.getRpcPort()
        && getPushPort() == o.getPushPort()
        && getFetchPort() == o.getFetchPort();
  }

  @Override
  public int hashCode() {
    return (id + epoch + getHost() + getRpcPort() + getPushPort() + getFetchPort()).hashCode();
  }

  @Override
  public String toString() {
    String peerAddr = "empty";
    if (peer != null) {
      peerAddr = peer.hostAndPorts();
    }
    return "PartitionLocation["
        + "\n  id-epoch:"
        + id
        + "-"
        + epoch
        + "\n  host-rpcPort-pushPort-fetchPort-replicatePort:"
        + getHost()
        + "-"
        + getRpcPort()
        + "-"
        + getPushPort()
        + "-"
        + getFetchPort()
        + "-"
        + getReplicatePort()
        + "\n  mode:"
        + mode
        + "\n  peer:("
        + peerAddr
        + ")\n  storage hint:"
        + storageInfo
        + "\n  mapIdBitMap:"
        + (mapIdBitMap == null ? EMPTY_MAP_ID_BITMAP : mapIdBitMap)
        + "]";
  }

  public WorkerInfo getWorker() {
    return new WorkerInfo(
        getHost(), getRpcPort(), getPushPort(), getFetchPort(), getReplicatePort());
  }

  public RoaringBitmap getMapIdBitMap() {
    return getMapIdBitMapOrCreate();
  }

  public RoaringBitmap getMapIdBitMapIfPresent() {
    return mapIdBitMap;
  }

  public RoaringBitmap getMapIdBitMapOrCreate() {
    RoaringBitmap current = mapIdBitMap;
    if (current == null) {
      synchronized (this) {
        current = mapIdBitMap;
        if (current == null) {
          current = new RoaringBitmap();
          mapIdBitMap = current;
        }
      }
    }
    return current;
  }

  public synchronized void setMapIdBitMap(RoaringBitmap mapIdBitMap) {
    this.mapIdBitMap = mapIdBitMap;
  }
}
