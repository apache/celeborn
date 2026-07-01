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

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.package$;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.client.security.CryptoHandler;

public class SparkCryptoHandlerSuiteJ {

  private byte[] key;
  private CryptoHandler handler;

  @Before
  public void setUp() {
    key = new byte[16];
    new SecureRandom().nextBytes(key);
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(package$.MODULE$.IO_ENCRYPTION_ENABLED(), true);
    handler = new SparkCryptoHandler(sparkConf, key);
  }

  @Test
  public void testRoundTrip() throws IOException {
    byte[] plaintext = "hello world, this is a test of encryption".getBytes();

    byte[] encrypted = handler.encrypt(plaintext, 0, plaintext.length);
    assertFalse(
        "Encrypted output should differ from plaintext", Arrays.equals(plaintext, encrypted));

    byte[] decrypted = handler.decrypt(encrypted, 0, encrypted.length);
    assertArrayEquals(plaintext, decrypted);
  }

  @Test
  public void testEncryptedDiffersFromPlaintext() throws IOException {
    byte[] plaintext = "deterministic test data for comparison".getBytes();

    byte[] encrypted = handler.encrypt(plaintext, 0, plaintext.length);
    assertFalse(
        "Encrypted output should differ from plaintext", Arrays.equals(plaintext, encrypted));
  }

  @Test
  public void testSameDataEncryptsThenDecrypts() throws IOException {
    byte[] plaintext = "same data encrypted twice".getBytes();

    byte[] encrypted1 = handler.encrypt(plaintext, 0, plaintext.length);
    byte[] encrypted2 = handler.encrypt(plaintext, 0, plaintext.length);

    // Both should decrypt to the same plaintext
    byte[] decrypted1 = handler.decrypt(encrypted1, 0, encrypted1.length);
    byte[] decrypted2 = handler.decrypt(encrypted2, 0, encrypted2.length);

    assertArrayEquals(plaintext, decrypted1);
    assertArrayEquals(plaintext, decrypted2);
  }

  @Test
  public void testEncryptWithOffset() throws IOException {
    byte[] actual = "offset test data".getBytes();
    int offset = 10;
    byte[] padded = new byte[offset + actual.length + 20];
    System.arraycopy(actual, 0, padded, offset, actual.length);

    byte[] encrypted = handler.encrypt(padded, offset, actual.length);
    byte[] decrypted = handler.decrypt(encrypted, 0, encrypted.length);

    assertArrayEquals(actual, decrypted);
  }

  @Test
  public void testDecryptWithWrongKeyFails() throws IOException {
    byte[] plaintext = "secret data".getBytes();
    byte[] encrypted = handler.encrypt(plaintext, 0, plaintext.length);

    byte[] wrongKey = new byte[16];
    new SecureRandom().nextBytes(wrongKey);
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(package$.MODULE$.IO_ENCRYPTION_ENABLED(), true);
    CryptoHandler wrongHandler = new SparkCryptoHandler(sparkConf, wrongKey);

    byte[] decrypted = null;
    try {
      decrypted = wrongHandler.decrypt(encrypted, 0, encrypted.length);
    } catch (IOException e) {
      // acceptable — some implementations throw on wrong key
      return;
    }
    // CryptoStreamUtils may return garbage instead of throwing
    assertFalse(
        "Decryption with wrong key should not produce original plaintext",
        Arrays.equals(plaintext, decrypted));
  }

  @Test
  public void testLargeData() throws IOException {
    byte[] plaintext = new byte[64 * 1024]; // 64KB
    new SecureRandom().nextBytes(plaintext);

    byte[] encrypted = handler.encrypt(plaintext, 0, plaintext.length);
    byte[] decrypted = handler.decrypt(encrypted, 0, encrypted.length);

    assertArrayEquals(plaintext, decrypted);
  }

  @Test
  public void testEmptyData() throws IOException {
    byte[] encrypted = handler.encrypt(new byte[0], 0, 0);

    byte[] decrypted = handler.decrypt(encrypted, 0, encrypted.length);
    assertEquals(0, decrypted.length);
  }

  /**
   * Verifies that the decrypt bounds check uses {@code length - 20} (4-byte length prefix + 16-byte
   * IV), not the previous {@code length - 4}. A crafted payload whose embedded length value is
   * between {@code length - 19} and {@code length - 5} (inclusive) must be rejected.
   */
  @Test
  public void testDecryptRejectsCraftedLengthBetweenLengthMinus4AndLengthMinus20()
      throws IOException {
    // Construct a minimal on-wire buffer: [4-byte length][16-byte IV][0-byte ciphertext].
    // Total = 20 bytes. Embed a plaintext length of 1 — valid under the old (length-4)
    // guard (1 <= 20-4=16) but invalid under the corrected (length-20) guard (1 > 20-20=0).
    int totalLen = 20; // 4 (length prefix) + 16 (IV) + 0 (ciphertext)
    byte[] crafted = new byte[totalLen];
    ByteBuffer.wrap(crafted).order(ByteOrder.BIG_ENDIAN).putInt(1); // claim 1 byte of plaintext

    try {
      handler.decrypt(crafted, 0, crafted.length);
      fail("Expected IOException for crafted length > length - 20");
    } catch (IOException e) {
      assertTrue(
          "Exception message should mention decrypted length",
          e.getMessage().contains("decrypted length") || e.getMessage().contains("Invalid"));
    }
  }
}
