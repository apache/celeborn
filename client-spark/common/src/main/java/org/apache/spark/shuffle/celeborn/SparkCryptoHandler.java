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

package org.apache.spark.shuffle.celeborn;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.security.CryptoStreamUtils;

import org.apache.celeborn.client.security.CryptoHandler;

public class SparkCryptoHandler implements CryptoHandler {
  // On-wire format: [4-byte plaintext length][16-byte IV][ciphertext].
  // The minimum overhead (length prefix + IV) added by the crypto stream.
  private static final int CRYPTO_OVERHEAD_BYTES =
      Integer.BYTES + CryptoStreamUtils.IV_LENGTH_IN_BYTES();

  private final SparkConf sparkConf;
  private final byte[] key;

  public SparkCryptoHandler(SparkConf sparkConf, byte[] key) {
    // Pre-filter sparkConf to only crypto-relevant keys so that
    // CryptoStreamUtils.toCryptoConf() does not scan the full SparkConf on every batch.
    Properties cryptoProps = CryptoStreamUtils.toCryptoConf(sparkConf);
    SparkConf minimalConf = new SparkConf(false);
    String prefix = CryptoStreamUtils.SPARK_IO_ENCRYPTION_COMMONS_CONFIG_PREFIX();
    for (String propKey : cryptoProps.stringPropertyNames()) {
      minimalConf.set(prefix + propKey, cryptoProps.getProperty(propKey));
    }
    this.sparkConf = minimalConf;
    this.key = key;
  }

  @Override
  public byte[] encrypt(byte[] input, int offset, int length) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeInt(length);
    try (OutputStream cos = CryptoStreamUtils.createCryptoOutputStream(dos, sparkConf, key)) {
      cos.write(input, offset, length);
    }
    return baos.toByteArray();
  }

  @Override
  public byte[] decrypt(byte[] input, int offset, int length) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(input, offset, length);
    DataInputStream dis = new DataInputStream(bais);
    int decryptedLength = dis.readInt();
    // The encrypted payload format is: [4-byte plaintext length][16-byte IV][ciphertext].
    // The minimum on-wire overhead is CRYPTO_OVERHEAD_BYTES (4 + 16 = 20), so the maximum
    // valid plaintext length is length - 20. A value outside this range indicates corruption
    // or a wrong key.
    if (decryptedLength < 0 || decryptedLength > length - CRYPTO_OVERHEAD_BYTES) {
      throw new IOException(
          "Invalid decrypted length: " + decryptedLength + ", encrypted length: " + length);
    }
    try (DataInputStream cis =
        new DataInputStream(CryptoStreamUtils.createCryptoInputStream(dis, sparkConf, key))) {
      byte[] decrypted = new byte[decryptedLength];
      cis.readFully(decrypted);
      return decrypted;
    }
  }
}
