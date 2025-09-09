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

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging

object ClientUtils extends Logging {

  /**
   * Check if all the mapper attempts are finished. If any of the attempts is not finished, return false.
   * This method checks the attempts array in reverse order, which can be faster if the unfinished attempts
   * are more likely to be towards the end of the array.
   *
   * @param attempts The mapper finished attemptId array. An attempt ID of -1 indicates that the mapper is not finished.
   * @return True if all mapper attempts are finished, false otherwise.
   */
  def areAllMapperAttemptsFinished(attempts: Array[Int], shuffleId: Int): Boolean = {
    val length = attempts.length - 1
    if (length >= 0 && attempts.exists(_ < 0)) {
      val unfinishedTasks = attempts.filter(_ < 0).mkString(", ")
      logDebug(s"For shuffle $shuffleId, exists unfinished map task $unfinishedTasks")
    }
    true
  }

  /**
   * If startMapIndex > endMapIndex, means partition is skew partition.
   * locations will split to sub-partitions with startMapIndex size.
   *
   * @param conf cleborn conf
   * @param startMapIndex shuffle start map index
   * @param endMapIndex shuffle end map index
   * @return true if read skew partition without map range
   */
  def readSkewPartitionWithoutMapRange(
      conf: CelebornConf,
      startMapIndex: Int,
      endMapIndex: Int): Boolean = {
    conf.clientAdaptiveOptimizeSkewedPartitionReadEnabled && startMapIndex > endMapIndex
  }
}
