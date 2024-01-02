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

import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client._
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.StatusCode

class ChangePartitionManagerTest extends AnyFunSuite {

  test("testPendingPartitionChangeRequests") {

    val conf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, "localhost1:9097,localhost2:9097")
    val App = "app-1"
    val lifecycleManager = new LifecycleManager(App, conf)
    val context = mock(classOf[RequestLocationCallContext])
    val oldPartition = mock(classOf[PartitionLocation])

    val manager = new ChangePartitionManager(conf, lifecycleManager)

    val shuffleId = 1
    val partitionId = 2
    val epoch = 3
    val cause = Some(StatusCode.SUCCESS)
    val request =
      ChangePartitionRequest(context, shuffleId, partitionId, epoch, oldPartition, cause)
    manager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      epoch,
      oldPartition,
      cause)

    manager.start()
    Thread.sleep(1000)

    val changeRequests = manager.getAllPartitionRequests(shuffleId)
    assert(!changeRequests.isEmpty)
    assert(changeRequests.contains(request))

    manager.stop()
  }
}
