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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.google.common.io.Files;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.JavaUtils;

/**
 * SSLFactory to initialize and configure use of JSSE for SSL in Celeborn.
 *
 * <p>Note: code was initially copied from Apache Spark.
 */
public class SSLFactory {
  private static final Logger logger = LoggerFactory.getLogger(SSLFactory.class);

  /** For a configuration specifying keystore/truststore files */
  private SSLContext jdkSslContext;

  private KeyManager[] keyManagers;
  private TrustManager[] trustManagers;
  private String requestedProtocol;
  private String[] requestedCiphers;

  private SSLFactory(final Builder b) {
    this.requestedProtocol = b.requestedProtocol;
    this.requestedCiphers = b.requestedCiphers;
    try {
      initJdkSslContext(b);
    } catch (Exception e) {
      throw new RuntimeException("SSLFactory creation failed", e);
    }
  }

  private void initJdkSslContext(final Builder b) throws IOException, GeneralSecurityException {
    // Validate invariants
    if (b.autoSslEnabled) {
      // this has been already validated - adding precondition check here since we are going to
      // overwrite these configs
      if (null != b.keyStore || null != b.trustStore) {
        throw new IllegalArgumentException(
            "keystore and truststore cant be configured for auto ssl");
      }
      configureAutoSsl(b);
    }

    this.keyManagers =
        null != b.keyStore ? keyManagers(b.keyStore, b.keyPassword, b.keyStorePassword) : null;
    this.trustManagers =
        trustStoreManagers(
            b.trustStore, b.trustStorePassword,
            b.trustStoreReloadingEnabled, b.trustStoreReloadIntervalMs);
    this.jdkSslContext = createSSLContext(requestedProtocol, keyManagers, trustManagers);
  }

  public List<KeyManager> getKeyManagers() {
    return null != keyManagers
        ? Collections.unmodifiableList(Arrays.asList(keyManagers))
        : Collections.emptyList();
  }

  public List<TrustManager> getTrustManagers() {
    return null != trustManagers
        ? Collections.unmodifiableList(Arrays.asList(trustManagers))
        : Collections.emptyList();
  }

  /*
   * As b.trustStore is null, credulousTrustStoreManagers will be used - and so all
   * certs will be accepted - and hence self-signed cert from lifecycle manager will
   * be accepted at executors: nothing else needs to be done to enable its
   * use - other than generation of the certificate itself, which is what gets done here.
   */
  private void configureAutoSsl(Builder b) {
    // The keystore creation ideally be done only at Lifecycle manager, and not in executors
    // It does not cause issues to run it at executors, though it is not necessary.
    // How can be identify this scenario and ensure this is not invoked at executors ?
    try {
      SSLUtils.SelfSignedCertificateConfig config = new SSLUtils.SelfSignedCertificateConfig();
      SSLUtils.generateSelfSignedCertificate(config);
      // Now that we have create the self signed cert, update the builder config
      b.keyStore(config.keystoreFile, config.keystorePassword).keyPassword(config.keyPassword);
    } catch (CertificateException | KeyStoreException | IOException | NoSuchAlgorithmException e) {
      // Unexpected
      throw new IllegalStateException(
          "Unable to create self signed certificate for configuring auto ssl", e);
    }
  }

  public boolean hasKeyManagers() {
    return null != keyManagers && keyManagers.length > 0;
  }

  public void destroy() {
    if (trustManagers != null) {
      for (int i = 0; i < trustManagers.length; i++) {
        if (trustManagers[i] instanceof ReloadingX509TrustManager) {
          try {
            ((ReloadingX509TrustManager) trustManagers[i]).destroy();
          } catch (InterruptedException ex) {
            logger.info("Interrupted while destroying trust manager: {}", ex, ex);
          }
        }
      }
      trustManagers = null;
    }

    keyManagers = null;
    jdkSslContext = null;
    requestedProtocol = null;
    requestedCiphers = null;
  }

  /** Builder class to construct instances of {@link SSLFactory} with specific options */
  public static class Builder {
    private String requestedProtocol;
    private String[] requestedCiphers;
    private File keyStore;
    private String keyStorePassword;
    private String keyPassword;
    private File trustStore;
    private String trustStorePassword;
    private boolean trustStoreReloadingEnabled;
    private int trustStoreReloadIntervalMs;
    private boolean autoSslEnabled;

    /**
     * Sets the requested protocol, i.e., "TLSv1.2", "TLSv1.1", etc
     *
     * @param requestedProtocol The requested protocol
     * @return The builder object
     */
    public Builder requestedProtocol(String requestedProtocol) {
      this.requestedProtocol = requestedProtocol == null ? "TLSv1.3" : requestedProtocol;
      return this;
    }

    /**
     * Sets the requested cipher suites, i.e., "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", etc
     *
     * @param requestedCiphers The requested ciphers
     * @return The builder object
     */
    public Builder requestedCiphers(String[] requestedCiphers) {
      this.requestedCiphers = requestedCiphers;
      return this;
    }

    /**
     * Sets the Keystore and Keystore password
     *
     * @param keyStore The key store file to use
     * @param keyStorePassword The password for the key store
     * @return The builder object
     */
    public Builder keyStore(File keyStore, String keyStorePassword) {
      this.keyStore = keyStore;
      this.keyStorePassword = keyStorePassword;
      return this;
    }

    /**
     * Sets the key password
     *
     * @param keyPassword The password for the private key in the key store
     * @return The builder object
     */
    public Builder keyPassword(String keyPassword) {
      this.keyPassword = keyPassword;
      return this;
    }

    /**
     * Sets the trust-store, trust-store password, whether to use a Reloading TrustStore, and the
     * trust-store reload interval, if enabled
     *
     * @param trustStore The trust store file to use
     * @param trustStorePassword The password for the trust store
     * @param trustStoreReloadingEnabled Whether trust store reloading is enabled
     * @param trustStoreReloadIntervalMs The interval at which to reload the trust store file
     * @return The builder object
     */
    public Builder trustStore(
        File trustStore,
        String trustStorePassword,
        boolean trustStoreReloadingEnabled,
        int trustStoreReloadIntervalMs) {
      this.trustStore = trustStore;
      this.trustStorePassword = trustStorePassword;
      this.trustStoreReloadingEnabled = trustStoreReloadingEnabled;
      this.trustStoreReloadIntervalMs = trustStoreReloadIntervalMs;
      return this;
    }

    /**
     * Enables auto ssl, which is applicable only to rpc_app transport module. When enabled,
     * keystore and trust store should not be configured. A self signed certificate is generated,
     * which is used for securing the communication - and on ssl client connection side, all
     * certificates will be accepted. This allows for configuring network encryption without needing
     * to provision certificates for each application.
     */
    public Builder autoSslEnabled(boolean autoSslEnabled) {
      this.autoSslEnabled = autoSslEnabled;
      return this;
    }

    /**
     * Builds our {@link SSLFactory}
     *
     * @return The built {@link SSLFactory}
     */
    public SSLFactory build() {
      return new SSLFactory(this);
    }
  }

  /**
   * Returns an initialized {@link SSLContext}
   *
   * @param requestedProtocol The requested protocol to use
   * @param keyManagers The list of key managers to use
   * @param trustManagers The list of trust managers to use
   * @return The built {@link SSLContext}
   * @throws GeneralSecurityException
   */
  private static SSLContext createSSLContext(
      String requestedProtocol, KeyManager[] keyManagers, TrustManager[] trustManagers)
      throws GeneralSecurityException {
    SSLContext sslContext = SSLContext.getInstance(requestedProtocol);
    sslContext.init(keyManagers, trustManagers, null);
    return sslContext;
  }

  /**
   * Creates a new {@link SSLEngine}. Note that currently client auth is not supported
   *
   * @param isClient Whether the engine is used in a client context
   * @param allocator The {@link ByteBufAllocator to use}
   * @return A valid {@link SSLEngine}.
   */
  public SSLEngine createSSLEngine(boolean isClient, ByteBufAllocator allocator) {
    SSLEngine engine = createEngine(isClient, allocator);
    engine.setUseClientMode(isClient);
    engine.setWantClientAuth(true);
    engine.setEnabledProtocols(enabledProtocols(engine, requestedProtocol));
    engine.setEnabledCipherSuites(enabledCipherSuites(engine, requestedCiphers));
    return engine;
  }

  private SSLEngine createEngine(boolean isClient, ByteBufAllocator allocator) {
    return jdkSslContext.createSSLEngine();
  }

  private static final X509Certificate[] EMPTY_CERT_ARRAY = new X509Certificate[0];

  private static TrustManager[] credulousTrustStoreManagers() {
    return new TrustManager[] {
      new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {}

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {}

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return EMPTY_CERT_ARRAY;
        }
      }
    };
  }

  private static TrustManager[] trustStoreManagers(
      File trustStore,
      String trustStorePassword,
      boolean trustStoreReloadingEnabled,
      int trustStoreReloadIntervalMs)
      throws IOException, GeneralSecurityException {
    if (trustStore == null || !trustStore.exists()) {
      return credulousTrustStoreManagers();
    } else {
      if (trustStoreReloadingEnabled) {
        ReloadingX509TrustManager reloading =
            new ReloadingX509TrustManager(
                KeyStore.getDefaultType(),
                trustStore,
                trustStorePassword,
                trustStoreReloadIntervalMs);
        reloading.init();
        return new TrustManager[] {reloading};
      } else {
        return defaultTrustManagers(trustStore, trustStorePassword);
      }
    }
  }

  public static TrustManager[] defaultTrustManagers(File trustStore, String trustStorePassword)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
    try (InputStream input = Files.asByteSource(trustStore).openStream()) {
      KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      char[] passwordCharacters =
          trustStorePassword != null ? trustStorePassword.toCharArray() : null;
      ks.load(input, passwordCharacters);
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      return tmf.getTrustManagers();
    }
  }

  private static KeyManager[] keyManagers(
      File keyStore, String keyPassword, String keyStorePassword)
      throws NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException,
          UnrecoverableKeyException {
    KeyManagerFactory factory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    char[] keyStorePasswordChars = keyStorePassword != null ? keyStorePassword.toCharArray() : null;
    char[] keyPasswordChars =
        keyPassword != null ? keyPassword.toCharArray() : keyStorePasswordChars;
    factory.init(loadKeyStore(keyStore, keyStorePasswordChars), keyPasswordChars);
    return factory.getKeyManagers();
  }

  private static KeyStore loadKeyStore(File keyStore, char[] keyStorePassword)
      throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
    if (keyStore == null) {
      throw new KeyStoreException(
          "keyStore cannot be null. Please configure celeborn.ssl.<module>.keyStore");
    }

    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    FileInputStream fin = new FileInputStream(keyStore);
    try {
      ks.load(fin, keyStorePassword);
      return ks;
    } finally {
      JavaUtils.closeQuietly(fin);
    }
  }

  private static String[] enabledProtocols(SSLEngine engine, String requestedProtocol) {
    String[] supportedProtocols = engine.getSupportedProtocols();
    String[] defaultProtocols = {"TLSv1.3", "TLSv1.2"};
    String[] enabledProtocols =
        ((requestedProtocol == null || requestedProtocol.isEmpty())
            ? defaultProtocols
            : new String[] {requestedProtocol});

    List<String> protocols = addIfSupported(supportedProtocols, enabledProtocols);
    if (!protocols.isEmpty()) {
      return protocols.toArray(new String[protocols.size()]);
    } else {
      return supportedProtocols;
    }
  }

  private static String[] enabledCipherSuites(
      String[] supportedCiphers, String[] defaultCiphers, String[] requestedCiphers) {
    String[] baseCiphers =
        new String[] {
          // We take ciphers from the mozilla modern list first (for TLS 1.3):
          // https://wiki.mozilla.org/Security/Server_Side_TLS
          "TLS_CHACHA20_POLY1305_SHA256",
          "TLS_AES_128_GCM_SHA256",
          "TLS_AES_256_GCM_SHA384",
          // Next we have the TLS1.2 ciphers for intermediate compatibility (since JDK8 does not
          // support TLS1.3)
          "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
          "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
          "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
          "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
          "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
          "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
          "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
          "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384"
        };
    String[] enabledCiphers =
        ((requestedCiphers == null || requestedCiphers.length == 0)
            ? baseCiphers
            : requestedCiphers);

    List<String> ciphers = addIfSupported(supportedCiphers, enabledCiphers);
    if (!ciphers.isEmpty()) {
      return ciphers.toArray(new String[ciphers.size()]);
    } else {
      // Use the default from JDK as fallback.
      return defaultCiphers;
    }
  }

  private static String[] enabledCipherSuites(SSLEngine engine, String[] requestedCiphers) {
    return enabledCipherSuites(
        engine.getSupportedCipherSuites(), engine.getEnabledCipherSuites(), requestedCiphers);
  }

  private static List<String> addIfSupported(String[] supported, String... names) {
    List<String> enabled = new ArrayList<>();
    Set<String> supportedSet = new HashSet<>(Arrays.asList(supported));
    for (String n : names) {
      if (supportedSet.contains(n)) {
        enabled.add(n);
      }
    }
    return enabled;
  }

  public static SSLFactory createSslFactory(TransportConf conf) {
    if (conf.sslEnabled()) {

      if (conf.sslEnabledAndKeysAreValid()) {
        return new SSLFactory.Builder()
            .requestedProtocol(conf.sslProtocol())
            .requestedCiphers(conf.sslRequestedCiphers())
            .autoSslEnabled(conf.autoSslEnabled())
            .keyStore(conf.sslKeyStore(), conf.sslKeyStorePassword())
            .trustStore(
                conf.sslTrustStore(),
                conf.sslTrustStorePassword(),
                conf.sslTrustStoreReloadingEnabled(),
                conf.sslTrustStoreReloadIntervalMs())
            .build();
      } else {
        logger.error(
            "SSL encryption enabled but keyStore is not configured for "
                + conf.getModuleName()
                + "! Please ensure the configured keys are present.");
        throw new IllegalArgumentException(
            conf.getModuleName()
                + " SSL encryption enabled for "
                + conf.getModuleName()
                + " but keyStore not configured !");
      }
    } else {
      return null;
    }
  }
}
