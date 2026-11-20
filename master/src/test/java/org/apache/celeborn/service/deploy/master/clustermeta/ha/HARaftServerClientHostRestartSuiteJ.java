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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import scala.Tuple2;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.CelebornRackResolver;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.rpc.RpcEnv;

/**
 * A client.host added to a cluster whose Raft data directory is kept must take effect: on restart
 * Ratis restores the persisted peer configuration, which still carries the old client address.
 */
public class HARaftServerClientHostRestartSuiteJ {

  private static final String[] IDS = {"1", "2", "3"};

  private final List<HARaftServer> servers = new ArrayList<>();

  @After
  public void stopServers() {
    stopAll();
  }

  @Test
  public void clientHostAppliesAfterRestartWithRetainedRaftData() throws Exception {
    int[] ratisPorts = freePorts();
    int[] rpcPorts = freePorts();
    File[] storageDirs = new File[IDS.length];
    for (int i = 0; i < IDS.length; i++) {
      storageDirs[i] = Files.createTempDirectory("celeborn-ratis-restart-" + IDS[i]).toFile();
    }

    startAll(ratisPorts, rpcPorts, storageDirs, false);
    awaitLeader();
    stopAll();
    // Ratis releases the storage directory lock asynchronously on close().
    Thread.sleep(5000);

    startAll(ratisPorts, rpcPorts, storageDirs, true);
    int leader = awaitLeader();
    HARaftServer follower = servers.get((leader + 1) % IDS.length);
    // isLeader() refreshes the cached leader endpoints as a side effect.
    Assert.assertFalse(follower.isLeader());

    Optional<HARaftServer.LeaderPeerEndpoints> endpoints =
        follower.getCachedLeaderPeerRpcEndpoint();
    Assert.assertTrue(endpoints.isPresent());
    String expected = clientHost(IDS[leader]) + ":" + rpcPorts[leader];
    Tuple2<String, String> rpcEndpoints = endpoints.get().rpcEndpoints;
    Assert.assertEquals(expected, rpcEndpoints._1);
    Assert.assertEquals(expected, rpcEndpoints._2);
  }

  private static String clientHost(String id) {
    return "master" + id + ".gateway.invalid";
  }

  private void startAll(int[] ratisPorts, int[] rpcPorts, File[] storageDirs, boolean clientHost)
      throws IOException {
    List<MasterNode> nodes = new ArrayList<>();
    for (int i = 0; i < IDS.length; i++) {
      MasterNode.Builder builder =
          new MasterNode.Builder()
              .setNodeId(IDS[i])
              .setHost("localhost")
              .setRatisPort(ratisPorts[i])
              .setRpcPort(rpcPorts[i])
              .setInternalRpcPort(rpcPorts[i]);
      if (clientHost) {
        builder.setClientHost(clientHost(IDS[i]));
      }
      nodes.add(builder.build());
    }
    RpcEnv rpcEnv = Mockito.mock(RpcEnv.class);
    when(rpcEnv.setupEndpointRef(any(), any())).thenReturn(Mockito.mock(RpcEndpointRef.class));
    for (int i = 0; i < IDS.length; i++) {
      CelebornConf conf = new CelebornConf();
      conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR().key(), storageDirs[i].getAbsolutePath());
      for (int j = 0; j < IDS.length; j++) {
        conf.set("celeborn.master.ha.node." + IDS[j] + ".host", "localhost");
        conf.set("celeborn.master.ha.node." + IDS[j] + ".port", String.valueOf(rpcPorts[j]));
        if (clientHost) {
          conf.set("celeborn.master.ha.node." + IDS[j] + ".client.host", clientHost(IDS[j]));
        }
      }
      HAMasterMetaManager statusSystem =
          new HAMasterMetaManager(rpcEnv, conf, new CelebornRackResolver(conf), null);
      List<MasterNode> peers = new ArrayList<>(nodes);
      MasterNode local = peers.remove(i);
      HARaftServer server =
          HARaftServer.newMasterRatisServer(new MetaHandler(statusSystem), conf, local, peers);
      statusSystem.setRatisServer(server);
      server.start();
      servers.add(server);
    }
  }

  private int awaitLeader() throws InterruptedException {
    for (int waited = 0; waited < 60000; waited += 500) {
      for (int i = 0; i < servers.size(); i++) {
        if (servers.get(i).isLeader()) {
          return i;
        }
      }
      Thread.sleep(500);
    }
    throw new AssertionError("no leader elected within 60s");
  }

  private void stopAll() {
    servers.forEach(HARaftServer::stop);
    servers.clear();
  }

  private static int[] freePorts() throws IOException {
    int[] ports = new int[IDS.length];
    for (int i = 0; i < ports.length; i++) {
      try (ServerSocket socket = new ServerSocket(0)) {
        ports[i] = socket.getLocalPort();
      }
    }
    return ports;
  }
}
