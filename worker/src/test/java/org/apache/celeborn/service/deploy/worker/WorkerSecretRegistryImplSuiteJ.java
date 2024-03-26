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

package org.apache.celeborn.service.deploy.worker;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.google.protobuf.GeneratedMessageV3;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.celeborn.common.client.MasterClient;
import org.apache.celeborn.common.protocol.PbApplicationAuthMeta;

public class WorkerSecretRegistryImplSuiteJ {

  private WorkerSecretRegistryImpl secretRegistry;

  private MasterClient masterClient = Mockito.mock(MasterClient.class);

  @Before
  public void setUp() {
    secretRegistry = new WorkerSecretRegistryImpl(10000);
    secretRegistry.initialize(masterClient);
  }

  @Test
  public void testGetSecretKeyCacheHit() throws Throwable {
    String appId = "testAppId";
    String secret = "testSecret";
    secretRegistry.register(appId, secret);
    assertEquals(secret, secretRegistry.getSecretKey(appId));
    verify(masterClient, never())
        .askSync((GeneratedMessageV3) any(), eq(PbApplicationAuthMeta.class));
  }

  @Test
  public void testGetSecretKeyCacheMiss() throws Throwable {
    String appId = "testAppId";
    String secret = "testSecret";
    when(masterClient.askSync((GeneratedMessageV3) any(), eq(PbApplicationAuthMeta.class)))
        .thenReturn(PbApplicationAuthMeta.newBuilder().setAppId(appId).setSecret(secret).build());
    assertEquals(secret, secretRegistry.getSecretKey(appId));
    verify(masterClient, times(1))
        .askSync((GeneratedMessageV3) any(), eq(PbApplicationAuthMeta.class));
  }

  @Test
  public void testRegistration() {
    String appId = "testAppId";
    String secret = "testSecret";
    secretRegistry.register(appId, secret);
    assertTrue(secretRegistry.isRegistered(appId));
    assertFalse(secretRegistry.isRegistered("nonExistentAppId"));
  }

  @Test
  public void testUnregistration() {
    String appId = "testAppId";
    String secret = "testSecret";
    secretRegistry.register(appId, secret);
    assertTrue(secretRegistry.isRegistered(appId));
    secretRegistry.unregister(appId);
    assertFalse(secretRegistry.isRegistered(appId));
  }
}
