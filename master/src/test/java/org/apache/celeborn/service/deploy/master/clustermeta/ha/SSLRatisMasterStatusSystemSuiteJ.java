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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.CelebornConf$;
import org.apache.celeborn.common.network.ssl.SSLFactory;
import org.apache.celeborn.common.network.ssl.SslSampleConfigs;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.util.Utils;

public class SSLRatisMasterStatusSystemSuiteJ extends RatisMasterStatusSystemSuiteJ {

  private static final CelebornConf confWithHostPreferred = new CelebornConf();

  static {
    confWithHostPreferred.set(CelebornConf$.MODULE$.NETWORK_BIND_PREFER_IP(), false);
  }

  private static class CertificateData {
    final File file;
    final KeyPair keyPair;
    final X509Certificate cert;

    // If caData is null, we are generating for CA - else for a cert which is using the ca
    // from caData
    CertificateData(CertificateData caData) throws Exception {
      this.file = File.createTempFile("file", ".jks");
      file.deleteOnExit();

      this.keyPair = SslSampleConfigs.generateKeyPair("RSA");

      // for both ca and cert, we are simply using the same machien as CN
      String hostname = Utils.localHostName(confWithHostPreferred);
      final String dn = "CN=" + hostname + ",O=MyCompany,C=US";

      if (null != caData) {
        this.cert =
            SslSampleConfigs.generateCertificate(
                dn,
                keyPair,
                365,
                "SHA256withRSA",
                false,
                new String[] {hostname},
                caData.keyPair,
                caData.cert);
        SslSampleConfigs.createKeyStore(
            file, "password", "password", "cert", keyPair.getPrivate(), cert);
      } else {
        this.cert =
            SslSampleConfigs.generateCertificate(
                dn, keyPair, 365, "SHA256withRSA", true, null, null, null);
        SslSampleConfigs.createTrustStore(file, "password", "ca", cert);
      }
    }
  }

  private static final AtomicReference<CertificateData> caData;

  static {
    try {
      caData = new AtomicReference<>(new CertificateData(null));
    } catch (Exception ex) {
      throw new IllegalStateException("Unable to initialize", ex);
    }
  }

  @BeforeClass
  public static void init() throws Exception {

    resetRaftServer(
        configureSsl(caData.get(), configureServerConf(new CelebornConf(), 1)),
        configureSsl(caData.get(), configureServerConf(new CelebornConf(), 2)),
        configureSsl(caData.get(), configureServerConf(new CelebornConf(), 3)),
        true);
  }

  static CelebornConf configureSsl(CertificateData ca, CelebornConf conf) throws Exception {
    conf.set("celeborn.master.ha.ratis.raft.rpc.type", "GRPC");

    CertificateData server = new CertificateData(ca);

    final String module = TransportModuleConstants.RPC_SERVICE_MODULE;

    conf.set("celeborn.ssl." + module + ".enabled", "true");
    conf.set("celeborn.ssl." + module + ".keyStore", server.file.getAbsolutePath());

    conf.set("celeborn.ssl." + module + ".keyStorePassword", "password");
    conf.set("celeborn.ssl." + module + ".keyPassword", "password");
    conf.set("celeborn.ssl." + module + ".privateKeyPassword", "password");
    conf.set("celeborn.ssl." + module + ".protocol", "TLSv1.2");
    conf.set("celeborn.ssl." + module + ".trustStore", ca.file.getAbsolutePath());
    conf.set("celeborn.ssl." + module + ".trustStorePassword", "password");

    return conf;
  }

  @Test
  public void testSslEnabled() throws Exception {
    assertTrue(isSslServer(RATISSERVER1.getRaftAddress(), RATISSERVER1.getRaftPort()));
    assertTrue(isSslServer(RATISSERVER2.getRaftAddress(), RATISSERVER2.getRaftPort()));
    assertTrue(isSslServer(RATISSERVER3.getRaftAddress(), RATISSERVER3.getRaftPort()));
  }

  // Validate if the server listening at the port is using TLS or not.
  static boolean isSslServer(InetAddress address, int port) throws Exception {
    try (SSLSocket socket = createSslSocket(address, port)) {
      socket.setSoTimeout(5000);
      socket.startHandshake();
      // handshake succeeded, this will always return true in this case
      return socket.getSession().isValid();
    }
  }

  private static SSLSocket createSslSocket(InetAddress address, int port) throws Exception {
    TrustManager trustStore = SSLFactory.defaultTrustManagers(caData.get().file, "password")[0];
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, new TrustManager[] {trustStore}, null);
    SSLSocketFactory factory = context.getSocketFactory();
    Socket socket = new Socket();
    socket.connect(new InetSocketAddress(address, port), 5000);
    socket.setSoTimeout(5000);

    return (SSLSocket) factory.createSocket(socket, address.getHostAddress(), port, true);
  }
}
