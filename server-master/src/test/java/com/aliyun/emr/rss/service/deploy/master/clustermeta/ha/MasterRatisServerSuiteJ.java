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

import java.util.UUID;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.util.LifeCycle;
import org.junit.Assert;
import org.junit.Test;

public class MasterRatisServerSuiteJ extends RatisBaseSuiteJ {

  /**
   * Start a Ratis Server and checks its state.
   */
  @Test
  public void testStartRatisServer() {
    Assert.assertEquals("Ratis Server should be in running state",
        LifeCycle.State.RUNNING, ratisServer.getServerState());
  }

  @Test
  public void verifyRaftGroupIdGenerationWithDefaultServiceId() {
    UUID uuid = UUID.nameUUIDFromBytes(DEFAULT_SERVICE_ID.getBytes());
    RaftGroupId raftGroupId = ratisServer.getRaftGroup().getGroupId();
    Assert.assertEquals(uuid, raftGroupId.getUuid());
    Assert.assertEquals(raftGroupId.toByteString().size(), 16);
  }

  @Test
  public void testIsLeader() {
    // since we just has one instance by default, isLeader will always be false,
    // we use this test case to verify api access correctness.
    Assert.assertTrue(!ratisServer.isLeader());
  }
}
