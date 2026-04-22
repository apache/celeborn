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

package org.apache.celeborn.common.serializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;

import org.junit.Test;

/** Tests for {@link JavaDeserializerFilter}. */
public class JavaDeserializerFilterSuiteJ {

  @Test
  public void testDefaultAllowedPackages() {
    JavaDeserializerFilter filter = JavaDeserializerFilter.create();
    assertTrue(filter.isClassAllowed("java.lang.String"));
    assertTrue(filter.isClassAllowed("scala.Option"));
    assertTrue(filter.isClassAllowed("org.apache.celeborn.Worker"));
    assertTrue(filter.isClassAllowed("com.google.protobuf.Message"));
    assertTrue(filter.isClassAllowed("[B"));
    assertTrue(filter.isClassAllowed("[Ljava.lang.String;"));
    assertFalse(filter.isClassAllowed("jdk.internal.misc.Unsafe"));
    assertFalse(filter.isClassAllowed("sun.misc.Unsafe"));
    assertFalse(filter.isClassAllowed("com.malicious.Payload"));
  }

  @Test
  public void testCustomAllowedPackages() {
    JavaDeserializerFilter filter =
        JavaDeserializerFilter.create(
            new String[] {"java.", "com.x."}, 50, 5000, 50000L, 50000000L);
    assertTrue(filter.isClassAllowed("java.lang.String"));
    assertFalse(filter.isClassAllowed("scala.Option"));
  }

  @Test
  public void testValidDeserialization() throws Exception {
    JavaDeserializerFilter filter = JavaDeserializerFilter.create();
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
    objectStream.writeObject("test");
    objectStream.close();
    Object result =
        filter
            .createValidatingInputStream(new ByteArrayInputStream(byteStream.toByteArray()), null)
            .readObject();
    assertEquals("test", result);
  }

  @Test
  public void testPrimitiveClassDeserialization() throws Exception {
    JavaDeserializerFilter filter = JavaDeserializerFilter.create();
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
    objectStream.writeObject(int.class);
    objectStream.close();
    Object result =
        filter
            .createValidatingInputStream(new ByteArrayInputStream(byteStream.toByteArray()), null)
            .readObject();
    assertEquals(int.class, result);
  }

  @Test(expected = InvalidClassException.class)
  public void testRejectBlockedClass() throws Exception {
    JavaDeserializerFilter.create()
        .createValidatingInputStream(new ByteArrayInputStream(createFakePayload("com.x.A")), null)
        .readObject();
  }

  private static byte[] createFakePayload(String className) throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(byteStream);
    dataStream.writeShort(0xACED);
    dataStream.writeShort(5);
    dataStream.writeByte(0x73);
    dataStream.writeByte(0x72);
    dataStream.writeShort(className.length());
    dataStream.writeBytes(className);
    dataStream.writeLong(1);
    dataStream.writeByte(2);
    dataStream.writeShort(0);
    dataStream.writeByte(0x78);
    dataStream.writeByte(0x70);
    return byteStream.toByteArray();
  }
}
