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

import org.apache.spark.SparkConf;
import org.apache.spark.security.CryptoStreamUtils;

import org.apache.celeborn.client.security.CryptoHandler;

public class SparkCryptoHandler implements CryptoHandler {
  private final SparkConf sparkConf;
  private final byte[] key;

  public SparkCryptoHandler(SparkConf sparkConf, byte[] key) {
    this.sparkConf = sparkConf;
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
    if (decryptedLength < 0) {
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
