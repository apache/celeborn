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

import org.apache.celeborn.client.read.CelebornIntegrityCheckTracker.{ensureIntegrityCheck, registerReader}

object ValidatingIterator {
  def empty[K, C](): Iterator[Product2[K, C]] = Iterator.empty
}

/**
 * Wraps a shuffle reader iterator and enforces that the data integrity check was performed for
 * every partition in [startPartition, endPartition) before the iterator is considered exhausted.
 *
 * On construction, registers itself with [[CelebornIntegrityCheckTracker]] so the executor
 * plugin can verify it was fully consumed. When [[hasNext]] first returns {@code false}, it
 * calls [[CelebornIntegrityCheckTracker.ensureIntegrityCheck]] to assert that every expected
 * partition received a matching [[CelebornIntegrityCheckTracker.registerValidation]] call from
 * its underlying [[org.apache.celeborn.client.read.CelebornInputStream]].
 */
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
