package org.apache.spark.shuffle.celeborn

import org.apache.celeborn.client.read.CelebornIntegrityCheckTracker.{ensureIntegrityCheck, registerReader}

object ValidatingIterator {
  def empty[K, C](): Iterator[Product2[K, C]] = Iterator.empty
}

class ValidatingIterator[K, C](
    delegate: Iterator[Product2[K, C]],
    shuffleId: Int,
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int) extends Iterator[Product2[K, C]] {

  require(delegate != null, "iterator cannot be null")

  private var closed = false

  registerReader(this)

  override def hasNext: Boolean = {
    if (closed) {
      false
    } else {
      val result = delegate.hasNext
      if (!result) {
        ensureIntegrityCheck(
          this,
          shuffleId,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition)
        closed = true
      }
      result
    }
  }

  override def next(): Product2[K, C] = delegate.next()

  override def toString: String = s"ValidatingIterator{" +
    s"shuffleId=$shuffleId, " +
    s"startMapIndex=$startMapIndex, " +
    s"endMapIndex=$endMapIndex, " +
    s"startPartition=$startPartition, " +
    s"endPartition=$endPartition}"
}
