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

package org.apache.celeborn.client

import org.apache.celeborn.common.util.TimeSlidingHub

class PartitionSplitNode(var value: Long) extends TimeSlidingHub.TimeSlidingNode {

  override def combineNode(node: TimeSlidingHub.TimeSlidingNode): Unit = {
    value += node.asInstanceOf[PartitionSplitNode].value
  }

  /** Minus the value from the {@param node}. */
  override def separateNode(node: TimeSlidingHub.TimeSlidingNode): Unit = {
    value -= node.asInstanceOf[PartitionSplitNode].value
  }

  override def clone: PartitionSplitNode = new PartitionSplitNode(value)
}

class PartitionSplitTimeSlidingHub(timeWindowsInSecs: Int, intervalPerBucketInMills: Int)
  extends TimeSlidingHub[PartitionSplitNode](timeWindowsInSecs, intervalPerBucketInMills) {
  override protected def newEmptyNode(): PartitionSplitNode = {
    new PartitionSplitNode(0)
  }

  def getActiveFullLocationSizeMBPerSec(): Int = {
    val currentSizeMB = sum().getLeft.value
    if (currentSizeMB > 0) {
      currentSizeMB.toInt / timeWindowsInSecs
    } else {
      0
    }
  }
}
