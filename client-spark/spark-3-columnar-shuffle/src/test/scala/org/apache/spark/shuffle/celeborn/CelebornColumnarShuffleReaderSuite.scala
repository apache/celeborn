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

package org.apache.spark.shuffle.celeborn

import org.apache.spark.{ShuffleDependency, SparkConf}
import org.apache.spark.serializer.{KryoSerializer, KryoSerializerInstance}
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.execution.columnar.CelebornColumnarBatchSerializerInstance
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.junit.Test
import org.mockito.{MockedStatic, Mockito}

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier

class CelebornColumnarShuffleReaderSuite {

  @Test
  def createColumnarShuffleReader(): Unit = {
    val handle = new CelebornShuffleHandle[Int, String, String](
      "appId",
      "host",
      0,
      new UserIdentifier("mock", "mock"),
      0,
      10,
      null)

    var shuffleClient: MockedStatic[ShuffleClient] = null
    try {
      shuffleClient = Mockito.mockStatic(classOf[ShuffleClient])
      val shuffleReader = SparkUtils.createColumnarShuffleReader(
        handle,
        0,
        10,
        0,
        10,
        null,
        new CelebornConf(),
        null)
      assert(shuffleReader.getClass == classOf[CelebornColumnarShuffleReader[Int, String]])
    } finally {
      if (shuffleClient != null) {
        shuffleClient.close()
      }
    }
  }

  @Test
  def columnarShuffleReaderNewSerializerInstance(): Unit = {
    var shuffleClient: MockedStatic[ShuffleClient] = null
    try {
      shuffleClient = Mockito.mockStatic(classOf[ShuffleClient])
      val shuffleReader = SparkUtils.createColumnarShuffleReader(
        new CelebornShuffleHandle[Int, String, String](
          "appId",
          "host",
          0,
          new UserIdentifier("mock", "mock"),
          0,
          10,
          null),
        0,
        10,
        0,
        10,
        null,
        new CelebornConf(),
        null)
      val shuffleDependency = Mockito.mock(classOf[ShuffleDependency[Int, String, String]])
      Mockito.when(shuffleDependency.shuffleId).thenReturn(0)
      Mockito.when(shuffleDependency.serializer).thenReturn(new KryoSerializer(
        new SparkConf(false)))

      // CelebornColumnarShuffleReader creates new serializer instance with dependency which has null schema.
      var serializerInstance = shuffleReader.newSerializerInstance(shuffleDependency)
      assert(serializerInstance.getClass == classOf[KryoSerializerInstance])

      // CelebornColumnarShuffleReader creates new serializer instance with dependency which has non-null schema.
      val dependencyUtils = Mockito.mockStatic(classOf[CustomShuffleDependencyUtils])
      dependencyUtils.when(() =>
        CustomShuffleDependencyUtils.getSchema(shuffleDependency)).thenReturn(new StructType().add(
        "key",
        IntegerType).add("value", StringType))
      Mockito.when(shuffleDependency.serializer).thenReturn(new UnsafeRowSerializer(2, null))
      serializerInstance = shuffleReader.newSerializerInstance(shuffleDependency)
      assert(serializerInstance.getClass == classOf[CelebornColumnarBatchSerializerInstance])
    } finally {
      if (shuffleClient != null) {
        shuffleClient.close()
      }
    }
  }
}
