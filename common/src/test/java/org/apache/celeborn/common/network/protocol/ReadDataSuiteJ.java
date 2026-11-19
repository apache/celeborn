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

package org.apache.celeborn.common.network.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

public class ReadDataSuiteJ {

  private static ByteBuf body() {
    return Unpooled.wrappedBuffer("testdata".getBytes());
  }

  @Test
  public void testReadDataHashCodeIsDerivedFromStreamIdAndBody() {
    ReadData readData = new ReadData(1L, body());
    assertEquals(Objects.hash(1L, readData.body()), readData.hashCode());
    // stable across calls, i.e. it does not depend on the identity of the message itself
    assertEquals(readData.hashCode(), readData.hashCode());
  }

  @Test
  public void testSubPartitionReadDataHashCodeIsDerivedFromStreamIdSubPartitionIdAndBody() {
    SubPartitionReadData readData = new SubPartitionReadData(1L, 2, body());
    assertEquals(Objects.hash(1L, 2, readData.body()), readData.hashCode());
    assertEquals(readData.hashCode(), readData.hashCode());
  }

  @Test
  public void testReadDataWithDifferentBodyAreNotEqual() {
    ReadData first = new ReadData(1L, body());
    ReadData second = new ReadData(1L, body());
    assertEquals(first, first);
    assertNotEquals(first, second);
  }
}
