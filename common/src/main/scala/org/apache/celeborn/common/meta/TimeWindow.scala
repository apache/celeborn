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

package org.apache.celeborn.common.meta

import java.util.concurrent.atomic.LongAdder

class TimeWindow(windowSize: Int, minWindowCount: Int) {
  protected val totalCount = new LongAdder
  protected val totalTime = new LongAdder
  protected val timeWindow = new Array[(Long, Long)](windowSize)
  protected var index = 0

  def update(delta: Long): Unit = {
    totalTime.add(delta)
    totalCount.increment()
  }

  def getAverage(): Long = {
    val currentFlushTime = totalTime.sumThenReset()
    val currentFlushCount = totalCount.sumThenReset()

    var totalFlushTime = 0L
    var totalFlushCount = 0L
    if (currentFlushCount >= minWindowCount) {
      timeWindow(index) = (currentFlushTime, currentFlushCount)
      index = (index + 1) % windowSize
    }

    timeWindow.foreach { case (flushTime, flushCount) =>
      totalFlushTime = totalFlushTime + flushTime
      totalFlushCount = totalFlushCount + flushCount
    }

    if (totalFlushCount != 0) {
      totalFlushTime / totalFlushCount
    } else {
      0L
    }
  }
}
