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

import static org.apache.celeborn.common.network.TestHelper.getResourceAsAbsolutePath;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.stream.Stream;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SslSampleConfigs {

  private static final Logger LOG = LoggerFactory.getLogger(SslSampleConfigs.class);

  static {
    Security.addProvider(new BouncyCastleProvider());
  }

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

  public static Map<String, String> createAutoSslConfigForModule(String module) {
    Map<String, String> confMap = new HashMap<>();
    confMap.put("celeborn.ssl." + module + ".enabled", "true");
    confMap.put("celeborn.ssl." + module + ".protocol", "TLSv1.2");
    confMap.put("celeborn.ssl." + module + ".autoSslEnabled", "true");
    return confMap;
  }

  private static Map<String, String> createConfigMapForModule(String module, boolean forDefault) {
    Map<String, String> confMap = new HashMap<>();
    confMap.put("celeborn.ssl." + module + ".enabled", "true");
    confMap.put("celeborn.ssl." + module + ".trustStoreReloadingEnabled", "false");
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
  public static X509Certificate generateCertificate(
      String dn, KeyPair pair, int days, String algorithm) throws Exception {
    return generateCertificate(dn, pair, days, algorithm, false, null, null, null);
  }

  /**
   * Create a self-signed X.509 Certificate.
   *
   * @param dn the X.509 Distinguished Name, eg "CN=Test, L=London, C=GB"
   * @param pair the KeyPair for the server
   * @param days how many days from now the Certificate is valid for
   * @param algorithm the signing algorithm, eg "SHA1withRSA"
   * @param generateCaCert Is this request to generate a CA cert
   * @param altNames Optional: Alternate names to be added to the cert - we add them as both
   *     hostnames and ip's.
   * @param caKeyPair Optional: the KeyPair of the CA, to be used to sign this certificate. caCert
   *     should also be specified to use it
   * @param caCert Optional: the CA cert, to be used to sign this certificate. caKeyPair should also
   *     be specified to use it
   * @return the signed certificate (signed using ca if provided, else self-signed)
   */
  @SuppressWarnings("deprecation")
  public static X509Certificate generateCertificate(
      String dn,
      KeyPair pair,
      int days,
      String algorithm,
      boolean generateCaCert,
      String[] altNames,
      KeyPair caKeyPair,
      X509Certificate caCert)
      throws Exception {

    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86400000L);
    BigInteger sn = new BigInteger(64, new SecureRandom());
    X500Name subjectName = new X500Name(dn);

    X500Name issuerName;
    KeyPair signingKeyPair;

    if (caKeyPair != null && caCert != null) {
      issuerName = new JcaX509CertificateHolder(caCert).getSubject();
      signingKeyPair = caKeyPair;
    } else {
      issuerName = subjectName;
      // self signed
      signingKeyPair = pair;
    }

    X509v3CertificateBuilder certBuilder =
        new JcaX509v3CertificateBuilder(
            issuerName, sn, from, to, new X500Name(dn), pair.getPublic());

    if (null != altNames) {
      Stream<GeneralName> dnsStream =
          Arrays.stream(altNames).map(h -> new GeneralName(GeneralName.dNSName, h));
      Stream<GeneralName> ipStream =
          Arrays.stream(altNames)
              .map(
                  h -> {
                    try {
                      return new GeneralName(GeneralName.iPAddress, h);
                    } catch (Exception ex) {
                      return null;
                    }
                  })
              .filter(Objects::nonNull);

      GeneralName[] arr = Stream.concat(dnsStream, ipStream).toArray(GeneralName[]::new);
      GeneralNames names = new GeneralNames(arr);

      certBuilder.addExtension(Extension.subjectAlternativeName, false, names);
      LOG.info("Added subjectAlternativeName extension for hosts : " + Arrays.toString(altNames));
    }

    if (generateCaCert) {
      certBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
      LOG.info("Added CA cert extension");
    }

    ContentSigner signer =
        new JcaContentSignerBuilder(algorithm).build(signingKeyPair.getPrivate());
    return new JcaX509CertificateConverter().getCertificate(certBuilder.build(signer));
  }

  public static KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(4096);
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
    KeyStore ks = KeyStore.getInstance("PKCS12");
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
}
