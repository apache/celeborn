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

    var shuffleClientClass: MockedStatic[ShuffleClient] = null
    try {
      shuffleClientClass = Mockito.mockStatic(classOf[ShuffleClient])
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
      if (shuffleClientClass != null) {
        shuffleClientClass.close()
      }
    }
  }
}
