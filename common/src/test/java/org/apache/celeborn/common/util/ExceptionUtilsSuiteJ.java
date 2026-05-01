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

package org.apache.celeborn.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import org.apache.celeborn.common.client.MasterNotLeaderException;

public class ExceptionUtilsSuiteJ {

  @Test
  public void identifyDirectMasterNotLeaderException() {
    assertTrue(
        ExceptionUtils.isMasterNotLeader(
            new MasterNotLeaderException(
                "celeborn-master-0:9097", "celeborn-master-1:9097", null)));
  }

  @Test
  public void identifySerializedRatisNotLeaderStackTrace() {
    IOException exception =
        new IOException(
            "java.io.IOException: org.apache.celeborn.common.exception.CelebornRuntimeException: "
                + "Server 0@group-47BEDE733167 is not the leader, suggested leader is: "
                + "3|celeborn-master-3.celeborn-master-svc.celeborn.svc.cluster.local:9872");

    assertTrue(ExceptionUtils.isMasterNotLeader(exception));
  }

  @Test
  public void ignoreOtherBootstrapFailures() {
    assertFalse(ExceptionUtils.isMasterNotLeader(new IOException("Connection reset by peer")));
  }

  @Test
  public void rootCauseMessageUsesFirstLineOnly() {
    IOException exception =
        new IOException("Exception in sendRpcSync", new IOException("first line\n\tat stack line"));

    assertEquals("first line", ExceptionUtils.getRootCauseMessage(exception));
  }

  @Test
  public void masterNotLeaderMessagePreservesClassNameWithoutStackTrace() {
    IOException exception =
        new IOException(
            "Exception in sendRpcSync",
            new MasterNotLeaderException(
                "celeborn-master-0:9097", MasterNotLeaderException.LEADER_NOT_PRESENTED, null));

    String message = ExceptionUtils.getMasterNotLeaderMessage(exception);
    assertTrue(message.contains("org.apache.celeborn.common.client.MasterNotLeaderException"));
    assertTrue(message.contains("leader is not present"));
    assertFalse(message.contains("\n"));
  }
}
