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

package org.apache.celeborn.service.deploy.master.clustermeta.ha;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class NodeDetails {
  private final String nodeId;
  private final InetSocketAddress rpcAddress;
  private final int rpcPort;
  private final int ratisPort;

  /** Constructs MasterNodeDetails object. */
  private NodeDetails(String nodeId, InetSocketAddress rpcAddr, int rpcPort, int ratisPort) {
    this.nodeId = nodeId;
    this.rpcAddress = rpcAddr;
    this.rpcPort = rpcPort;
    this.ratisPort = ratisPort;
  }

  @Override
  public String toString() {
    return "MasterNodeDetails["
        + "nodeId="
        + nodeId
        + ", rpcAddress="
        + rpcAddress
        + ", rpcPort="
        + rpcPort
        + ", ratisPort="
        + ratisPort
        + "]";
  }

  /** Builder class for MasterNodeDetails. */
  public static class Builder {
    private String nodeId;
    private InetSocketAddress rpcAddress;
    private int rpcPort;
    private int ratisPort;

    public Builder setRpcAddress(InetSocketAddress rpcAddr) {
      this.rpcAddress = rpcAddr;
      this.rpcPort = rpcAddress.getPort();
      return this;
    }

    public Builder setRatisPort(int port) {
      this.ratisPort = port;
      return this;
    }

    public Builder setNodeId(String nodeId) {
      this.nodeId = nodeId;
      return this;
    }

    public NodeDetails build() {
      return new NodeDetails(nodeId, rpcAddress, rpcPort, ratisPort);
    }
  }

  public String getNodeId() {
    return nodeId;
  }

  public InetSocketAddress getRpcAddress() {
    return rpcAddress;
  }

  public boolean isHostUnresolved() {
    return rpcAddress.isUnresolved();
  }

  public InetAddress getInetAddress() {
    return rpcAddress.getAddress();
  }

  public String getHostName() {
    return rpcAddress.getHostName();
  }

  public String getRatisHostPortStr() {
    return getHostName() + ":" + ratisPort;
  }

  public int getRatisPort() {
    return ratisPort;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  public String getRpcAddressString() {
    return rpcAddress.getHostName() + ":" + rpcAddress.getPort();
  }
}
