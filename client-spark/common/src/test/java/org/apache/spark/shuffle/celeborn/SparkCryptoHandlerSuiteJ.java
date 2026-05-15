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
    byte[] padded = Arrays.copyOf(actual, actual.length + 20);

    byte[] encrypted = handler.encrypt(padded, 0, actual.length);
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
}
