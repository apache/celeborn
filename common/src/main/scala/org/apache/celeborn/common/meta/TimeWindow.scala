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
