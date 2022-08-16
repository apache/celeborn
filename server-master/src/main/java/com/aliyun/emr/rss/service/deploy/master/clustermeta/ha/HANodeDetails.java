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

package com.aliyun.emr.rss.service.deploy.master.clustermeta.ha;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.common.RssConf;

public class HANodeDetails {

  public static final Logger LOG = LoggerFactory.getLogger(HANodeDetails.class);
  private final NodeDetails localNodeDetails;
  private final List<NodeDetails> peerNodeDetails;

  public HANodeDetails(
      NodeDetails localNodeDetails,
      List<NodeDetails> peerNodeDetails) {
    this.localNodeDetails = localNodeDetails;
    this.peerNodeDetails = peerNodeDetails;
  }

  public NodeDetails getLocalNodeDetails() {
    return localNodeDetails;
  }

  public List<NodeDetails> getPeerNodeDetails() {
    return peerNodeDetails;
  }

  public static HANodeDetails loadHAConfig(RssConf conf) throws IllegalArgumentException {
    InetSocketAddress localRpcAddress = null;
    String localServiceId = null;
    String localNodeId = null;
    int localRatisPort = 0;

    String serviceId = conf.get(RssConf.HA_SERVICE_ID_KEY());
    if (serviceId == null) {
      throwConfException("Configuration has no %s service id.",
          RssConf.HA_SERVICE_ID_KEY());
      return null;
    }

    String nodeIdsStr = conf.get(RssConf.concatKeySuffix(RssConf.HA_NODES_KEY(), serviceId));
    Collection<String> nodeIds;
    if (nodeIdsStr != null) {
      nodeIds = Arrays.asList(nodeIdsStr.split(","));
    } else {
      throwConfException("Configuration does not have any value set for %s " +
          "for the service %s. List of Node ID's should be specified " +
          "for an Master service", RssConf.HA_NODES_KEY(), serviceId);
      return null;
    }

    List<NodeDetails> peerNodesList = new ArrayList<>();
    for (String nodeId : nodeIds) {
      String rpcAddrKey = RssConf.concatKeySuffix(RssConf.concatKeySuffix(
          RssConf.HA_ADDRESS_KEY(), serviceId), nodeId);
      String rpcAddrStr = conf.get(rpcAddrKey);
      if (rpcAddrStr == null || rpcAddrStr.isEmpty()) {
        throwConfException("Configuration does not have any value set for " +
            "%s. Ratis RPC Address should be set for all nodes.", rpcAddrKey);
        return null;
      }

      String ratisPortKey = RssConf.concatKeySuffix(RssConf.concatKeySuffix(
          RssConf.HA_RATIS_PORT_KEY(), serviceId), nodeId);
      int ratisPort = conf.getInt(ratisPortKey, RssConf.HA_RATIS_PORT_DEFAULT());

      InetSocketAddress addr = null;
      try {
        addr = NetUtils.createSocketAddr(rpcAddrStr, ratisPort);
      } catch (Exception e) {
        LOG.error("Couldn't create socket address for {} : {}", nodeId,
            rpcAddrStr, e);
        throw e;
      }

      if (addr.isUnresolved()) {
        LOG.error("Address for {} : {} couldn't be resolved. Proceeding " +
            "with unresolved host to create Ratis ring.", nodeId, rpcAddrStr);
      }

      if (!addr.isUnresolved() && isLocalAddress(addr.getAddress())) {
        localRpcAddress = addr;
        localServiceId = serviceId;
        localNodeId = nodeId;
        localRatisPort = ratisPort;
      } else {
        peerNodesList.add(getHANodeDetails(serviceId, nodeId, addr, ratisPort));
      }
    }
    return new HANodeDetails(getHANodeDetails(localServiceId,
        localNodeId, localRpcAddress, localRatisPort), peerNodesList);
  }

  /**
   * Create Local Node Details.
   * @param serviceId - Service ID this ratis server belongs to,
   * @param nodeId - Node ID of this ratis server.
   * @param rpcAddress - Rpc Address of the ratis server.
   * @param ratisPort - Ratis port of the ratis server.
   * @return MasterNodeDetails
   */
  public static NodeDetails getHANodeDetails(
      String serviceId, String nodeId, InetSocketAddress rpcAddress, int ratisPort) {
    Preconditions.checkNotNull(serviceId);
    Preconditions.checkNotNull(nodeId);

    return new NodeDetails.Builder()
        .setServiceId(serviceId)
        .setNodeId(nodeId)
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .build();
  }

  private static void throwConfException(String message, String... arguments)
      throws IllegalArgumentException {
    String exceptionMsg = String.format(message, arguments);
    LOG.error(exceptionMsg);
    throw new IllegalArgumentException(exceptionMsg);
  }

  public static boolean isLocalAddress(InetAddress addr) {
    boolean local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();
    if (!local) {
      try {
        local = NetworkInterface.getByInetAddress(addr) != null;
      } catch (SocketException var3) {
        local = false;
      }
    }

    return local;
  }
}
