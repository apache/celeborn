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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Enumeration;

import org.junit.Test;

public class SSLUtilsSuite {
  @Test
  public void testSelfSignedCertificateGeneration() throws Exception {
    File keystoreFile = File.createTempFile("keystore-", ".jks");
    keystoreFile.deleteOnExit();

    String alias = "my-cert";
    String keyPassword = "keypassword";
    String storePassword = "storePassword";

    SSLUtils.SelfSignedCertificateConfig config =
        new SSLUtils.SelfSignedCertificateConfig(keystoreFile, alias, storePassword, keyPassword);

    SSLUtils.generateSelfSignedCertificate(config);

    // Once jks has been created, validate its contents
    try (FileInputStream fis = new FileInputStream(keystoreFile)) {
      KeyStore keystore = KeyStore.getInstance("JKS");
      keystore.load(fis, storePassword.toCharArray());
      assertEquals(1, keystore.size());

      Enumeration<String> aliasEnumeration = keystore.aliases();
      assertTrue(aliasEnumeration.hasMoreElements());
      assertEquals(alias, aliasEnumeration.nextElement());
      assertFalse(aliasEnumeration.hasMoreElements());

      Certificate certificate = keystore.getCertificate(alias);
      assertNotNull(certificate);
      Key key = keystore.getKey(alias, keyPassword.toCharArray());
      assertNotNull(key);
    }
  }
}
