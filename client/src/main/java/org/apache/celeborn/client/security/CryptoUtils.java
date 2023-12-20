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

package org.apache.celeborn.client.security;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.crypto.cipher.CryptoCipher;
import org.apache.commons.crypto.random.CryptoRandomFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CryptoUtils {
  private static Logger logger = LoggerFactory.getLogger(CryptoUtils.class);
  public static final int IV_LENGTH_IN_BYTES = 16;
  public static final String COMMONS_CRYPTO_CONFIG_PREFIX = "commons.crypto.";
  public static final String COMMONS_CRYPTO_CONFIG_TRANSFORMATION =
      COMMONS_CRYPTO_CONFIG_PREFIX + "cipher.transformation";
  public static final String CRYPTO_ALGORITHM = "AES";

  public static byte[] createIoCryptoInitializationVector() {
    byte[] iv = new byte[IV_LENGTH_IN_BYTES];
    long initialIVStart = System.nanoTime();
    try {
      CryptoRandomFactory.getCryptoRandom(new Properties()).nextBytes(iv);
    } catch (GeneralSecurityException e) {
      logger.warn("Failed to create crypto Initialization Vector", e);
      iv = "1234567890123456".getBytes(StandardCharsets.UTF_8);
    }
    long initialIVFinish = System.nanoTime();
    long initialIVTime = TimeUnit.NANOSECONDS.toMillis(initialIVFinish - initialIVStart);
    if (initialIVTime > 2000) {
      logger.warn(
          "It costs {} milliseconds to create the Initialization Vector used by crypto",
          initialIVTime);
    }
    return iv;
  }

  public static CryptoCipher getEncipher(
      Optional<byte[]> ioCryptoKey, Properties ioCryptoConf, byte[] ioCryptoInitializationVector)
      throws IOException {
    CryptoCipher encipher = null;
    if (ioCryptoKey.isPresent()) {
      SecretKeySpec keySpec = new SecretKeySpec(ioCryptoKey.get(), CRYPTO_ALGORITHM);
      String transformation = (String) ioCryptoConf.get(COMMONS_CRYPTO_CONFIG_TRANSFORMATION);
      try (final CryptoCipher _encipher =
          org.apache.commons.crypto.utils.Utils.getCipherInstance(transformation, ioCryptoConf)) {
        encipher = _encipher;
        try {
          encipher.init(
              Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(ioCryptoInitializationVector));
        } catch (GeneralSecurityException e) {
          throw new IOException("Failed to init encipher", e);
        }
      }
    }
    return encipher;
  }

  public static int encrypt(
      CryptoCipher encipher, byte[] input, int offset, int length, byte[] output)
      throws IOException {
    try {
      int updateBytes = encipher.update(input, offset, length, output, 0);
      int finalBytes = encipher.doFinal(input, 0, 0, output, updateBytes);
      return updateBytes + finalBytes;
    } catch (ShortBufferException | BadPaddingException | IllegalBlockSizeException e) {
      throw new IOException("Failed to encrypt", e);
    }
  }

  public static CryptoCipher getDecipher(
      Optional<byte[]> key, Properties cryptoProp, byte[] cryptoInitilizationVector)
      throws IOException {
    CryptoCipher decipher = null;
    if (key.isPresent()) {
      SecretKeySpec keySpec = new SecretKeySpec(key.get(), CRYPTO_ALGORITHM);
      String transformation = (String) cryptoProp.get(COMMONS_CRYPTO_CONFIG_TRANSFORMATION);
      try (final CryptoCipher _decipher =
          org.apache.commons.crypto.utils.Utils.getCipherInstance(transformation, cryptoProp)) {
        decipher = _decipher;
        try {
          decipher.init(
              Cipher.DECRYPT_MODE, keySpec, new IvParameterSpec(cryptoInitilizationVector));
        } catch (GeneralSecurityException e) {
          throw new IOException("Failed to init encipher", e);
        }
      }
    }
    return decipher;
  }

  public static int decrypt(
      CryptoCipher decipher, byte[] input, int offset, int length, byte[] decoded)
      throws IOException {
    try {
      return decipher.doFinal(input, offset, length, decoded, 0);
    } catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException e) {
      throw new IOException("Failed to decrypt", e);
    }
  }
}
