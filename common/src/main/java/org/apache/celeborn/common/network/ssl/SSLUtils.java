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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.UUID;

import io.netty.handler.ssl.util.SelfSignedCertificate;

/** SSL util functions for use with SSLFactory */
class SSLUtils {

  static final String KEY_PAIR_ALGORITHM =
      System.getProperty("celeborn.SSLUtils.KEY_PAIR_ALGORITHM", "RSA");
  static final int KEY_PAIR_STRENGTH =
      Integer.getInteger("celeborn.SSLUtils.KEY_PAIR_STRENGTH", 2048);
  static final long CERTIFICATE_VALIDITY =
      // default, 1 year validity
      Long.getLong("celeborn.SSLUtils.CERTIFICATE_VALIDITY_DAYS", 365) * 24 * 60 * 60 * 1000;

  static class SelfSignedCertificateConfig {
    final File keystoreFile;
    final String alias;
    final String keystorePassword;
    final String keyPassword;

    SelfSignedCertificateConfig() throws IOException {
      this(
          createTempJksFile(),
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString(),
          UUID.randomUUID().toString());
    }

    SelfSignedCertificateConfig(
        File keystoreFile, String alias, String keystorePassword, String keyPassword) {
      this.keystoreFile = keystoreFile;
      this.alias = alias;
      this.keystorePassword = keystorePassword;
      this.keyPassword = keyPassword;
    }

    private static File createTempJksFile() throws IOException {
      File keystoreFile = File.createTempFile("auto-ssl-", ".jks");
      keystoreFile.deleteOnExit();
      return keystoreFile;
    }
  }

  static void generateSelfSignedCertificate(SelfSignedCertificateConfig config)
      throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
    SelfSignedCertificate cert = new SelfSignedCertificate();
    PrivateKey privateKey = cert.key();
    X509Certificate certificate = cert.cert();

    // Create a KeyStore
    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, null); // Initialize empty keystore

    keyStore.setKeyEntry(
        config.alias,
        privateKey,
        config.keyPassword.toCharArray(),
        new X509Certificate[] {certificate});

    // Save the keystore
    try (FileOutputStream outputStream = new FileOutputStream(config.keystoreFile)) {
      keyStore.store(outputStream, config.keystorePassword.toCharArray());
    }
  }
}
