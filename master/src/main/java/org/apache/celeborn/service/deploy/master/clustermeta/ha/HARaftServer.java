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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage.StartupOption;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.client.MasterClient;
import org.apache.celeborn.common.exception.CelebornRuntimeException;
import org.apache.celeborn.common.network.ssl.SSLFactory;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos.ResourceResponse;

public class HARaftServer {
  public static final Logger LOG = LoggerFactory.getLogger(HARaftServer.class);

  public static final UUID CELEBORN_UUID =
      UUID.nameUUIDFromBytes("CELEBORN".getBytes(StandardCharsets.UTF_8));

  public static final RaftGroupId RAFT_GROUP_ID = RaftGroupId.valueOf(CELEBORN_UUID);

  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();

  static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  private final MasterNode localNode;
  private final InetSocketAddress ratisAddr;
  private final String rpcEndpoint;
  private final String internalRpcEndpoint;
  private final RaftServer server;
  private final RaftGroup raftGroup;
  private final RaftPeerId raftPeerId;

  private final ClientId clientId = ClientId.randomId();

  private final MetaHandler metaHandler;

  private final StateMachine masterStateMachine;

  private final ScheduledExecutorService scheduledRoleChecker =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-ratis-role-checker");
  private long roleCheckIntervalMs;
  private final ReentrantReadWriteLock roleCheckLock = new ReentrantReadWriteLock();
  private Optional<RaftProtos.RaftPeerRole> cachedPeerRole = Optional.empty();
  private Optional<LeaderPeerEndpoints> cachedLeaderPeerRpcEndpoints = Optional.empty();

  private final CelebornConf conf;
  private long workerTimeoutDeadline;
  private long appTimeoutDeadline;

  /**
   * Returns a Master Ratis server.
   *
   * @param conf configuration
   * @param localRaftPeerId raft peer id of this Ratis server
   * @param localNode local node of this Ratis server
   * @param raftPeers peer nodes in the raft ring
   * @throws IOException
   */
  private HARaftServer(
      MetaHandler metaHandler,
      CelebornConf conf,
      RaftPeerId localRaftPeerId,
      MasterNode localNode,
      List<RaftPeer> raftPeers)
      throws IOException {
    this.metaHandler = metaHandler;
    this.localNode = localNode;
    this.ratisAddr = localNode.ratisAddr();
    this.rpcEndpoint = localNode.rpcEndpoint();
    this.internalRpcEndpoint = localNode.internalRpcEndpoint();
    this.raftPeerId = localRaftPeerId;
    this.raftGroup = RaftGroup.valueOf(RAFT_GROUP_ID, raftPeers);
    this.masterStateMachine = getStateMachine();
    this.conf = conf;

    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(conf.haMasterRatisRpcType());
    RaftProperties serverProperties = newRaftProperties(conf, rpc);
    Parameters sslParameters =
        localNode.sslEnabled() ? configureSsl(conf, serverProperties, rpc) : null;
    setDeadlineTime(Integer.MAX_VALUE, Integer.MAX_VALUE); // for default
    this.server =
        RaftServer.newBuilder()
            .setServerId(this.raftPeerId)
            .setGroup(this.raftGroup)
            .setProperties(serverProperties)
            .setParameters(sslParameters)
            .setStateMachine(masterStateMachine)
            // RATIS-1677. Do not auto format RaftStorage in RECOVER.
            .setOption(StartupOption.valueOf(conf.haMasterRatisStorageStartupOption()))
            .build();

    StringBuilder raftPeersStr = new StringBuilder();
    for (RaftPeer peer : raftPeers) {
      raftPeersStr.append(", ").append(peer.getAddress());
    }
    LOG.info(
        "Ratis server started with GroupID: {} and Raft Peers: {}.",
        RAFT_GROUP_ID,
        raftPeersStr.substring(2));

    // Run a scheduler to check and update the server role on the leader periodically
    this.scheduledRoleChecker.scheduleWithFixedDelay(
        () -> {
          // Run this check only on the leader OM
          if (cachedPeerRole.isPresent()
              && cachedPeerRole.get() == RaftProtos.RaftPeerRole.LEADER) {
            updateServerRole();
          }
        },
        roleCheckIntervalMs,
        roleCheckIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  public static HARaftServer newMasterRatisServer(
      MetaHandler metaHandler, CelebornConf conf, MasterNode localNode, List<MasterNode> peerNodes)
      throws IOException {
    String nodeId = localNode.nodeId();
    RaftPeerId localRaftPeerId = RaftPeerId.getRaftPeerId(nodeId);

    InetSocketAddress ratisAddr = localNode.ratisAddr();
    RaftPeer localRaftPeer =
        RaftPeer.newBuilder()
            .setId(localRaftPeerId)
            .setAddress(ratisAddr)
            .setClientAddress(localNode.rpcEndpoint())
            // We use admin address to host the internal rpc address
            .setAdminAddress(localNode.internalRpcEndpoint())
            .build();
    List<RaftPeer> raftPeers = new ArrayList<>();
    // Add this Ratis server to the Ratis ring
    raftPeers.add(localRaftPeer);
    peerNodes.forEach(
        peer -> {
          String peerNodeId = peer.nodeId();
          RaftPeerId raftPeerId = RaftPeerId.valueOf(peerNodeId);
          RaftPeer raftPeer;
          if (peer.isRatisHostUnresolved()) {
            raftPeer =
                RaftPeer.newBuilder()
                    .setId(raftPeerId)
                    .setAddress(peer.ratisEndpoint())
                    .setClientAddress(peer.rpcEndpoint())
                    // We use admin address to host the internal rpc address
                    .setAdminAddress(peer.internalRpcEndpoint())
                    .build();
          } else {
            InetSocketAddress peerRatisAddr = peer.ratisAddr();
            raftPeer =
                RaftPeer.newBuilder()
                    .setId(raftPeerId)
                    .setAddress(peerRatisAddr)
                    .setClientAddress(peer.rpcEndpoint())
                    // We use admin address to host the internal rpc address
                    .setAdminAddress(peer.internalRpcEndpoint())
                    .build();
          }

          // Add other nodes belonging to the same service to the Ratis ring
          raftPeers.add(raftPeer);
        });

    return new HARaftServer(metaHandler, conf, localRaftPeerId, localNode, raftPeers);
  }

  public ResourceResponse submitRequest(ResourceProtos.ResourceRequest request)
      throws CelebornRuntimeException {
    String requestId = request.getRequestId();
    Tuple2<String, Long> decoded = MasterClient.decodeRequestId(requestId);
    if (decoded == null) {
      throw new CelebornRuntimeException(
          "RequestId:" + requestId + " invalid, should be: uuid#callId.");
    }
    ClientId clientId = ClientId.valueOf(UUID.fromString(decoded._1));
    long callId = decoded._2;
    RaftClientRequest raftClientRequest =
        new RaftClientRequest.Builder()
            .setClientId(clientId)
            .setServerId(server.getId())
            .setGroupId(RAFT_GROUP_ID)
            .setCallId(callId)
            .setType(RaftClientRequest.writeRequestType())
            .setMessage(Message.valueOf(HAHelper.convertRequestToByteString(request)))
            .build();

    RaftClientReply raftClientReply;
    try {
      raftClientReply = server.submitClientRequestAsync(raftClientRequest).get();
    } catch (Exception ex) {
      throw new CelebornRuntimeException(ex.getMessage(), ex);
    }

    if (!raftClientReply.isSuccess()) {
      RaftException exception = raftClientReply.getException();
      throw new CelebornRuntimeException(
          exception == null ? "Encounter raft exception." : exception.getMessage());
    }

    try {
      byte[] bytes = raftClientReply.getMessage().getContent().toByteArray();
      return ResourceResponse.newBuilder(ResourceResponse.parseFrom(bytes)).build();
    } catch (InvalidProtocolBufferException ex) {
      throw new CelebornRuntimeException(ex.getMessage(), ex);
    }
  }

  /**
   * Start the Ratis server.
   *
   * @throws IOException
   */
  public void start() throws IOException {
    LOG.info(
        "Starting {} {} at port {}",
        getClass().getSimpleName(),
        server.getId(),
        ratisAddr.getPort());
    server.start();
  }

  public void stop() {
    try {
      server.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private RaftProperties newRaftProperties(CelebornConf conf, RpcType rpc) {
    final RaftProperties properties = new RaftProperties();
    // Set RPC type
    RaftConfigKeys.Rpc.setType(properties, rpc);

    // Set the ratis port number
    if (rpc == SupportedRpcType.GRPC) {
      GrpcConfigKeys.Server.setPort(properties, ratisAddr.getPort());
    } else if (rpc == SupportedRpcType.NETTY) {
      NettyConfigKeys.Server.setPort(properties, ratisAddr.getPort());
    }

    // Set Ratis storage directory
    String storageDir = conf.haMasterRatisStorageDir();
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(new File(storageDir)));

    // Set RAFT segment size
    long raftSegmentSize = conf.haMasterRatisLogSegmentSizeMax();
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(raftSegmentSize));
    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);

    // Set RAFT segment pre-allocated size
    long raftSegmentPreallocatedSize = conf.haMasterRatisLogPreallocatedSize();
    long raftSegmentWriteBufferSize = conf.haMasterRatisLogWriteBufferSize();
    int logAppenderQueueNumElements = conf.haMasterRatisLogAppenderQueueNumElements();
    long logAppenderQueueByteLimit = conf.haMasterRatisLogAppenderQueueBytesLimit();
    // RATIS-589. Eliminate buffer copying in SegmentedRaftLogOutputStream.
    // 4 bytes (serialized size) + logAppenderQueueByteLimit + 4 bytes (checksum)
    if (raftSegmentWriteBufferSize < logAppenderQueueByteLimit + 8) {
      throw new IllegalArgumentException(
          CelebornConf.HA_MASTER_RATIS_LOG_WRITE_BUFFER_SIZE().key()
              + " (= "
              + raftSegmentWriteBufferSize
              + ") is less than "
              + CelebornConf.HA_MASTER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT().key()
              + " + 8 (= "
              + (logAppenderQueueByteLimit + 8)
              + ")");
    }
    boolean shouldInstallSnapshot = conf.haMasterRatisLogInstallSnapshotEnabled();
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(
        properties, logAppenderQueueNumElements);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(
        properties, SizeInBytes.valueOf(logAppenderQueueByteLimit));
    RaftServerConfigKeys.Log.setPreallocatedSize(
        properties, SizeInBytes.valueOf(raftSegmentPreallocatedSize));
    RaftServerConfigKeys.Log.setWriteBufferSize(
        properties, SizeInBytes.valueOf(raftSegmentWriteBufferSize));
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, shouldInstallSnapshot);
    int logPurgeGap = conf.haMasterRatisLogPurgeGap();
    RaftServerConfigKeys.Log.setPurgeGap(properties, logPurgeGap);

    // For grpc set the maximum message size
    GrpcConfigKeys.setMessageSizeMax(properties, SizeInBytes.valueOf(logAppenderQueueByteLimit));

    // Set the server request timeout
    TimeDuration serverRequestTimeout =
        TimeDuration.valueOf(conf.haMasterRatisRpcRequestTimeout(), TimeUnit.SECONDS);
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties, serverRequestTimeout);

    // Set timeout for server retry cache entry
    TimeDuration retryCacheExpiryTime =
        TimeDuration.valueOf(conf.haMasterRatisRetryCacheExpiryTime(), TimeUnit.SECONDS);
    RaftServerConfigKeys.RetryCache.setExpiryTime(properties, retryCacheExpiryTime);

    // Set the server min and max timeout
    TimeDuration rpcTimeoutMin =
        TimeDuration.valueOf(conf.haMasterRatisRpcTimeoutMin(), TimeUnit.SECONDS);
    TimeDuration rpcTimeoutMax =
        TimeDuration.valueOf(conf.haMasterRatisRpcTimeoutMax(), TimeUnit.SECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, rpcTimeoutMin);
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, rpcTimeoutMax);

    TimeDuration firstElectionTimeoutMin =
        TimeDuration.valueOf(conf.haMasterRatisFirstElectionTimeoutMin(), TimeUnit.SECONDS);
    TimeDuration firstElectionTimeoutMax =
        TimeDuration.valueOf(conf.haMasterRatisFirstElectionTimeoutMax(), TimeUnit.SECONDS);
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties, firstElectionTimeoutMin);
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties, firstElectionTimeoutMax);

    // Set the rpc client timeout
    TimeDuration clientRpcTimeout =
        TimeDuration.valueOf(conf.haMasterRatisClientRpcTimeout(), TimeUnit.SECONDS);
    TimeDuration clientRpcWatchTimeout =
        TimeDuration.valueOf(conf.haMasterRatisClientRpcWatchTimeout(), TimeUnit.SECONDS);
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties, clientRpcTimeout);
    RaftClientConfigKeys.Rpc.setWatchRequestTimeout(properties, clientRpcWatchTimeout);

    // Set the number of maximum cached segments
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(properties, 2);

    TimeDuration noLeaderTimeout =
        TimeDuration.valueOf(conf.haMasterRatisNotificationNoLeaderTimeout(), TimeUnit.SECONDS);
    RaftServerConfigKeys.Notification.setNoLeaderTimeout(properties, noLeaderTimeout);
    TimeDuration slownessTimeout =
        TimeDuration.valueOf(conf.haMasterRatisRpcSlownessTimeout(), TimeUnit.SECONDS);
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties, slownessTimeout);

    // Set role checker time
    this.roleCheckIntervalMs = conf.haMasterRatisRoleCheckInterval();

    // snapshot retention
    int numSnapshotRetentionFileNum = conf.haMasterRatisSnapshotRetentionFileNum();
    RaftServerConfigKeys.Snapshot.setRetentionFileNum(properties, numSnapshotRetentionFileNum);

    // snapshot interval
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(
        properties, conf.haMasterRatisSnapshotAutoTriggerEnabled());

    long snapshotAutoTriggerThreshold = conf.haMasterRatisSnapshotAutoTriggerThreshold();
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, snapshotAutoTriggerThreshold);

    for (Map.Entry<String, String> ratisEntry : conf.haRatisCustomConfigs().entrySet()) {
      properties.set(ratisEntry.getKey().replace("celeborn.ratis.", ""), ratisEntry.getValue());
    }

    return properties;
  }

  private Parameters configureSsl(CelebornConf conf, RaftProperties properties, RpcType rpc) {

    if (rpc != SupportedRpcType.GRPC) {
      LOG.error(
          "SSL has been disabled for Raft communication between masters. "
              + "This is only supported when ratis is configured with GRPC");
      return null;
    }

    // This is used only for querying state after initialization - not actual SSL
    // also why nThreads does not matter
    SSLFactory factory =
        SSLFactory.createSslFactory(
            Utils.fromCelebornConf(conf, TransportModuleConstants.RPC_SERVICE_MODULE, 1));

    assert (null != factory);
    assert (factory.hasKeyManagers());
    assert (!factory.getTrustManagers().isEmpty());

    TrustManager trustManager = factory.getTrustManagers().get(0);
    KeyManager keyManager = factory.getKeyManagers().get(0);

    Parameters params = new Parameters();
    GrpcConfigKeys.TLS.setEnabled(properties, true);
    GrpcConfigKeys.TLS.setConf(params, new GrpcTlsConfig(keyManager, trustManager, true));

    LOG.info("SSL enabled for ratis communication between masters");

    return params;
  }

  private StateMachine getStateMachine() {
    StateMachine stateMachine = new StateMachine(this);
    stateMachine.setRaftGroupId(RAFT_GROUP_ID);
    return stateMachine;
  }

  public MetaHandler getMetaHandler() {
    return metaHandler;
  }

  @VisibleForTesting
  public LifeCycle.State getServerState() {
    return server.getLifeCycleState();
  }

  public RaftGroup getRaftGroup() {
    return this.raftGroup;
  }

  public StateMachine getMasterStateMachine() {
    return this.masterStateMachine;
  }

  /**
   * Check the cached leader status.
   *
   * @return true if cached role is Leader, false otherwise.
   */
  private boolean checkCachedPeerRoleIsLeader() {
    this.roleCheckLock.readLock().lock();
    try {
      return cachedPeerRole.isPresent() && cachedPeerRole.get() == RaftProtos.RaftPeerRole.LEADER;
    } finally {
      this.roleCheckLock.readLock().unlock();
    }
  }

  /**
   * Check if the current node is the leader node.
   *
   * @return true if Leader, false otherwise.
   */
  public boolean isLeader() {
    if (checkCachedPeerRoleIsLeader()) {
      return true;
    }

    // Get the server role from ratis server and update the cached values.
    updateServerRole();

    // After updating the server role, check and return if leader or not.
    return checkCachedPeerRoleIsLeader();
  }

  /**
   * Get the suggested leader peer id.
   *
   * @return RaftPeerId of the suggested leader node - {@code Optional<LeaderPeerEndpoints>}
   */
  public Optional<LeaderPeerEndpoints> getCachedLeaderPeerRpcEndpoint() {
    this.roleCheckLock.readLock().lock();
    try {
      return cachedLeaderPeerRpcEndpoints;
    } finally {
      this.roleCheckLock.readLock().unlock();
    }
  }

  /**
   * Get the group info (peer role and leader peer id) from Ratis server and update the server role.
   */
  public void updateServerRole() {
    try {
      GroupInfoReply groupInfo = getGroupInfo();
      RaftProtos.RoleInfoProto roleInfoProto = groupInfo.getRoleInfoProto();
      RaftProtos.RaftPeerRole thisNodeRole = roleInfoProto.getRole();
      Tuple2<String, String> leaderPeerRpcEndpoint = null;
      Tuple2<String, String> leaderPeerInternalRpcEndpoint = null;
      if (thisNodeRole.equals(RaftProtos.RaftPeerRole.LEADER)) {
        // Current Node always uses original rpcEndpoint/internalRpcEndpoint, as if something wrong
        // they would never return to client.
        setServerRole(
            thisNodeRole,
            Tuple2.apply(this.rpcEndpoint, this.rpcEndpoint),
            Tuple2.apply(this.internalRpcEndpoint, this.internalRpcEndpoint));
      } else if (thisNodeRole.equals(RaftProtos.RaftPeerRole.FOLLOWER)) {
        ByteString leaderNodeId = roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getId();
        // There may be a chance, here we get leaderNodeId as null. For
        // example, in 3 node Ratis, if 2 nodes are down, there will
        // be no leader.
        if (leaderNodeId != null && !leaderNodeId.isEmpty()) {
          String clientAddress =
              roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getClientAddress();
          leaderPeerRpcEndpoint = Utils.addressToIpHostAddressPair(clientAddress);
          // We use admin address to host the internal rpc address
          if (conf.internalPortEnabled()) {
            String adminAddress =
                roleInfoProto.getFollowerInfo().getLeaderInfo().getId().getAdminAddress();
            leaderPeerInternalRpcEndpoint = Utils.addressToIpHostAddressPair(adminAddress);
          } else {
            leaderPeerInternalRpcEndpoint = leaderPeerRpcEndpoint;
          }
        }

        setServerRole(thisNodeRole, leaderPeerRpcEndpoint, leaderPeerInternalRpcEndpoint);

      } else {
        setServerRole(thisNodeRole, null, null);
      }
    } catch (IOException e) {
      LOG.error(
          "Failed to retrieve RaftPeerRole. Setting cached role to "
              + "{} and resetting leader info.",
          RaftProtos.RaftPeerRole.UNRECOGNIZED,
          e);
      setServerRole(null, null, null);
    }
  }

  /** Set the current server role and the leader peer rpc endpoint. */
  private void setServerRole(
      RaftProtos.RaftPeerRole currentRole,
      Tuple2<String, String> leaderPeerRpcEndpoint,
      Tuple2<String, String> leaderPeerInternalRpcEndpoint) {
    this.roleCheckLock.writeLock().lock();
    try {
      boolean leaderChanged = false;
      if (RaftProtos.RaftPeerRole.LEADER == currentRole && !checkCachedPeerRoleIsLeader()) {
        leaderChanged = true;
        setDeadlineTime(conf.workerHeartbeatTimeout(), conf.appHeartbeatTimeoutMs());
      } else if (RaftProtos.RaftPeerRole.LEADER != currentRole && checkCachedPeerRoleIsLeader()) {
        leaderChanged = true;
        setDeadlineTime(Integer.MAX_VALUE, Integer.MAX_VALUE); // for revoke
      }

      if (leaderChanged) {
        LOG.warn(
            "Raft Role changed, CurrentNode Role: {}, Leader: {}",
            currentRole,
            leaderPeerRpcEndpoint);
      }

      this.cachedPeerRole = Optional.ofNullable(currentRole);
      if (null != leaderPeerRpcEndpoint) {
        this.cachedLeaderPeerRpcEndpoints =
            Optional.of(
                new LeaderPeerEndpoints(leaderPeerRpcEndpoint, leaderPeerInternalRpcEndpoint));
      } else {
        this.cachedLeaderPeerRpcEndpoints = Optional.empty();
      }
    } finally {
      this.roleCheckLock.writeLock().unlock();
    }
  }

  public GroupInfoReply getGroupInfo() throws IOException {
    GroupInfoRequest groupInfoRequest =
        new GroupInfoRequest(clientId, raftPeerId, RAFT_GROUP_ID, nextCallId());
    return server.getGroupInfo(groupInfoRequest);
  }

  // Exposed for testing
  public InetAddress getRaftAddress() {
    return this.ratisAddr.getAddress();
  }

  public int getRaftPort() {
    return this.ratisAddr.getPort();
  }

  public String getRpcEndpoint() {
    return this.rpcEndpoint;
  }

  public String getInternalRpcEndpoint() {
    return this.internalRpcEndpoint;
  }

  void stepDown() {
    try {
      TransferLeadershipRequest request =
          new TransferLeadershipRequest(
              clientId,
              server.getId(),
              raftGroup.getGroupId(),
              CallId.getAndIncrement(),
              null,
              60_000);
      RaftClientReply reply = server.transferLeadership(request);
      if (reply.isSuccess()) {
        LOG.info("Successfully step down leader {}.", server.getId());
      } else {
        LOG.warn("Step down leader failed!");
      }
    } catch (Exception e) {
      LOG.warn("Step down leader failed!", e);
    }
  }

  public void setDeadlineTime(long increaseWorkerTime, long increaseAppTime) {
    this.workerTimeoutDeadline = System.currentTimeMillis() + increaseWorkerTime;
    this.appTimeoutDeadline = System.currentTimeMillis() + increaseAppTime;
  }

  public long getWorkerTimeoutDeadline() {
    return workerTimeoutDeadline;
  }

  public long getAppTimeoutDeadline() {
    return appTimeoutDeadline;
  }

  @VisibleForTesting
  public RaftServer getServer() {
    return server;
  }

  public static class LeaderPeerEndpoints {
    // the rpcEndpoints Tuple2 (ip:port, host:port)
    public final Tuple2<String, String> rpcEndpoints;

    // the rpcInternalEndpoints Tuple2 (ip:port, host:port)
    public final Tuple2<String, String> rpcInternalEndpoints;

    public LeaderPeerEndpoints(
        Tuple2<String, String> rpcEndpoints, Tuple2<String, String> rpcInternalEndpoints) {
      this.rpcEndpoints = rpcEndpoints;
      this.rpcInternalEndpoints = rpcInternalEndpoints;
    }
  }
}
