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

object ClientUtils {

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
