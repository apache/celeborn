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

package org.apache.celeborn.common.network.ssl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLEngine;

import io.netty.buffer.ByteBufAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.TestHelper;
import org.apache.celeborn.common.network.util.TransportConf;

/** Tests that client engines carry the peer hostname so the ClientHello includes SNI. */
public class SSLFactorySuiteJ {

  private static final String TEST_MODULE = "rpc";
  // SNI carries DNS names only, so this must be multi-label and not an IP literal.
  private static final String PEER_HOST = "celeborn-master-1.example.com";
  private static final int PEER_PORT = 9097;

  private SSLFactory factory;

  @Before
  public void setUp() {
    CelebornConf celebornConf = new CelebornConf();
    TestHelper.updateCelebornConfWithMap(
        celebornConf, SslSampleConfigs.createDefaultConfigMapForModule(TEST_MODULE));
    factory = SSLFactory.createSslFactory(new TransportConf(TEST_MODULE, celebornConf));
    assertNotNull(factory);
  }

  @After
  public void tearDown() {
    if (factory != null) {
      factory.destroy();
      factory = null;
    }
  }

  private static void assertSniHost(SSLEngine engine, String expectedHost) {
    List<SNIServerName> serverNames = engine.getSSLParameters().getServerNames();
    assertNotNull("expected an SNI server_name entry", serverNames);
    assertEquals(1, serverNames.size());
    assertEquals(new SNIHostName(expectedHost), serverNames.get(0));
  }

  private static void assertNoSni(SSLEngine engine) {
    List<SNIServerName> serverNames = engine.getSSLParameters().getServerNames();
    assertTrue("expected no SNI server_name entry", serverNames == null || serverNames.isEmpty());
  }

  @Test
  public void testClientEngineSendsSniForPeerHost() {
    SSLEngine engine =
        factory.createSSLEngine(true, ByteBufAllocator.DEFAULT, PEER_HOST, PEER_PORT);
    assertEquals(PEER_HOST, engine.getPeerHost());
    assertEquals(PEER_PORT, engine.getPeerPort());
    assertSniHost(engine, PEER_HOST);
  }

  @Test
  public void testClientEngineWithoutPeerHostOmitsSni() {
    assertNoSni(factory.createSSLEngine(true, ByteBufAllocator.DEFAULT, null, PEER_PORT));
  }

  @Test
  public void testClientEngineWithoutPeerPortOmitsSni() {
    assertNoSni(factory.createSSLEngine(true, ByteBufAllocator.DEFAULT, PEER_HOST, -1));
  }

  @Test
  public void testClientEngineOmitsSniForIpLiteral() {
    // RFC 6066 disallows IP literals in server_name, so the JDK drops the extension.
    assertNoSni(factory.createSSLEngine(true, ByteBufAllocator.DEFAULT, "127.0.0.1", PEER_PORT));
  }

  @Test
  public void testServerEngineOmitsSni() {
    assertNoSni(factory.createSSLEngine(false, ByteBufAllocator.DEFAULT, PEER_HOST, PEER_PORT));
  }

  @Test
  public void testExistingOverloadOmitsSni() {
    assertNoSni(factory.createSSLEngine(true, ByteBufAllocator.DEFAULT));
  }
}
