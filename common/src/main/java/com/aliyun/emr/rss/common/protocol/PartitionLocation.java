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

import com.aliyun.emr.rss.common.meta.WorkerInfo;

public class PartitionLocation implements Serializable {
  public enum Mode {
    Master(0), Slave(1);

    private final byte mode;

    Mode(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.mode = (byte) id;
    }

    public byte mode() { return mode; }
  };

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
  private Mode mode;
  private PartitionLocation peer;

  public PartitionLocation(PartitionLocation loc) {
    this.reduceId = loc.reduceId;
    this.epoch = loc.epoch;
    this.host = loc.host;
    this.rpcPort = loc.rpcPort;
    this.pushPort = loc.pushPort;
    this.fetchPort = loc.fetchPort;
    this.mode = loc.mode;
    this.peer = loc.peer;
  }

  public PartitionLocation(
          int reduceId,
          int epoch,
          String host,
          int rpcPort,
          int pushPort,
          int fetchPort,
          Mode mode) {
    this(reduceId, epoch, host, rpcPort, pushPort, fetchPort, mode, null);
  }

  public PartitionLocation(
      int reduceId,
      int epoch,
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      Mode mode,
      PartitionLocation peer) {
    this.reduceId = reduceId;
    this.epoch = epoch;
    this.host = host;
    this.rpcPort = rpcPort;
    this.pushPort = pushPort;
    this.fetchPort = fetchPort;
    this.mode = mode;
    this.peer = peer;
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
    return "PartitionLocation[" + reduceId + "-" + epoch + " " + host + ":" + rpcPort
        + ":" + pushPort + ":" + fetchPort + " Mode: " + mode + " peer: " + peerAddr + "]";
  }

  public WorkerInfo getWorker() {
    WorkerInfo info = new WorkerInfo(host, rpcPort, pushPort, fetchPort);
    return info;
  }
}
