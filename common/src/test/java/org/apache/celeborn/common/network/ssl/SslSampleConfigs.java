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
import java.math.BigInteger;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.x500.X500Principal;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.x509.X509V1CertificateGenerator;

public class SslSampleConfigs {

  public static final String DEFAULT_KEY_STORE_PATH = getResourceAsAbsolutePath("/ssl/server.jks");
  public static final String SECOND_KEY_STORE_PATH =
      getResourceAsAbsolutePath("/ssl/server_another.jks");

  // trust store has ca's for both keys.
  public static final String TRUST_STORE_PATH = getResourceAsAbsolutePath("/ssl/truststore.jks");

  // this is a trust store which does not have either the primary or second cert's ca
  public static final String TRUST_STORE_WITHOUT_CA =
      getResourceAsAbsolutePath("/ssl/truststore-without-ca.jks");

  public static Map<String, String> createDefaultConfigMapForModule(String module) {
    return createConfigMapForModule(module, true);
  }

  public static Map<String, String> createAnotherConfigMapForModule(String module) {
    return createConfigMapForModule(module, false);
  }

  private static Map<String, String> createConfigMapForModule(String module, boolean forDefault) {
    Map<String, String> confMap = new HashMap<>();
    confMap.put("celeborn.ssl." + module + ".enabled", "true");
    confMap.put("celeborn.ssl." + module + ".trustStoreReloadingEnabled", "false");
    confMap.put("celeborn.ssl." + module + ".openSslEnabled", "false");
    confMap.put("celeborn.ssl." + module + ".trustStoreReloadIntervalMs", "10000");
    if (forDefault) {
      confMap.put("celeborn.ssl." + module + ".keyStore", DEFAULT_KEY_STORE_PATH);
    } else {
      confMap.put("celeborn.ssl." + module + ".keyStore", SECOND_KEY_STORE_PATH);
    }
    confMap.put("celeborn.ssl." + module + ".keyStorePassword", "password");
    confMap.put("celeborn.ssl." + module + ".keyPassword", "password");
    confMap.put("celeborn.ssl." + module + ".privateKeyPassword", "password");
    confMap.put("celeborn.ssl." + module + ".protocol", "TLSv1.2");
    confMap.put("celeborn.ssl." + module + ".trustStore", TRUST_STORE_PATH);
    confMap.put("celeborn.ssl." + module + ".trustStorePassword", "password");
    return confMap;
  }

  public static void createTrustStore(
      File trustStore, String password, String alias, Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setCertificateEntry(alias, cert);
    saveKeyStore(ks, trustStore, password);
  }

  /** Creates a keystore with multiple keys and saves it to a file. */
  public static <T extends Certificate> void createTrustStore(
      File trustStore, String password, Map<String, T> certs)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    for (Map.Entry<String, T> cert : certs.entrySet()) {
      ks.setCertificateEntry(cert.getKey(), cert.getValue());
    }
    saveKeyStore(ks, trustStore, password);
  }

  /**
   * Create a self-signed X.509 Certificate.
   *
   * @param dn the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
   * @param pair the KeyPair
   * @param days how many days from now the Certificate is valid for
   * @param algorithm the signing algorithm, eg "SHA1withRSA"
   * @return the self-signed certificate
   */
  @SuppressWarnings("deprecation")
  public static X509Certificate generateCertificate(
      String dn, KeyPair pair, int days, String algorithm)
      throws CertificateEncodingException, InvalidKeyException, IllegalStateException,
          NoSuchAlgorithmException, SignatureException {

    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86400000L);
    BigInteger sn = new BigInteger(64, new SecureRandom());
    KeyPair keyPair = pair;
    X509V1CertificateGenerator certGen = new X509V1CertificateGenerator();
    X500Principal dnName = new X500Principal(dn);

    certGen.setSerialNumber(sn);
    certGen.setIssuerDN(dnName);
    certGen.setNotBefore(from);
    certGen.setNotAfter(to);
    certGen.setSubjectDN(dnName);
    certGen.setPublicKey(keyPair.getPublic());
    certGen.setSignatureAlgorithm(algorithm);

    X509Certificate cert = certGen.generate(pair.getPrivate());
    return cert;
  }

  public static KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(1024);
    return keyGen.genKeyPair();
  }

  /**
   * Creates a keystore with a single key and saves it to a file.
   *
   * @param keyStore File keystore to save
   * @param password String store password to set on keystore
   * @param keyPassword String key password to set on key
   * @param alias String alias to use for the key
   * @param privateKey Key to save in keystore
   * @param cert Certificate to use as certificate chain associated to key
   * @throws GeneralSecurityException for any error with the security APIs
   * @throws IOException if there is an I/O error saving the file
   */
  public static void createKeyStore(
      File keyStore,
      String password,
      String keyPassword,
      String alias,
      Key privateKey,
      Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, keyPassword.toCharArray(), new Certificate[] {cert});
    saveKeyStore(ks, keyStore, password);
  }

  public static void createKeyStore(
      File keyStore, String password, String alias, Key privateKey, Certificate cert)
      throws GeneralSecurityException, IOException {
    KeyStore ks = createEmptyKeyStore();
    ks.setKeyEntry(alias, privateKey, password.toCharArray(), new Certificate[] {cert});
    saveKeyStore(ks, keyStore, password);
  }

  private static KeyStore createEmptyKeyStore() throws GeneralSecurityException, IOException {
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null, null); // initialize
    return ks;
  }

  private static void saveKeyStore(KeyStore ks, File keyStore, String password)
      throws GeneralSecurityException, IOException {
    // Write the file atomically to ensure tests don't read a partial write
    File tempFile = File.createTempFile("temp-key-store", "jks");
    FileOutputStream out = new FileOutputStream(tempFile);
    try {
      ks.store(out, password.toCharArray());
      out.close();
      Files.move(
          tempFile.toPath(),
          keyStore.toPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);
    } finally {
      out.close();
    }
  }

  public static String getResourceAsAbsolutePath(String path) {
    try {
      File tempFile = File.createTempFile(new File(path).getName(), null);
      tempFile.deleteOnExit();
      URL url = SslSampleConfigs.class.getResource(path);
      if (null == url) {
        throw new IllegalArgumentException("Unable to find " + path);
      }
      FileUtils.copyInputStreamToFile(url.openStream(), tempFile);
      return tempFile.getCanonicalPath();
    } catch (IOException e) {
      throw new RuntimeException("Failed to resolve path " + path, e);
    }
  }
}
