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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;

import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// In Tez shuffle, MapOutput encapsulates the logic to fetch map task's output data via http.
// So, in RSS, we should bypass this logic, and directly write data to MapOutput.
public class CelebornTezBypassWriter {
  private static final Logger LOG = LoggerFactory.getLogger(CelebornTezBypassWriter.class);

  public static void write(MapOutput mapOutput, byte[] buffer) {
    LOG.info(
        "RssTezBypassWriter write mapOutput, type:{}, buffer length:{}",
        mapOutput.getType(),
        buffer.length);
    // Write and commit uncompressed data to MapOutput.
    // In the majority of cases, merger allocates memory to accept data,
    // but when data size exceeds the threshold, merger can also allocate disk.
    // So, we should consider the two situations, respectively.
    if (mapOutput.getType() == MapOutput.Type.MEMORY) {
      byte[] memory = mapOutput.getMemory();
      System.arraycopy(buffer, 0, memory, 0, buffer.length);
    } else if (mapOutput.getType() == MapOutput.Type.DISK) {
      throw new IllegalStateException(
          "Celeborn map reduce client do not support OnDiskMapOutput. Try to increase mapreduce.reduce.shuffle.memory.limit.percent");
    } else {
      throw new IllegalStateException(
          "Merger reserve unknown type of MapOutput: " + mapOutput.getClass().getCanonicalName());
    }
  }

  public static void write(final FetchedInput mapOutput, byte[] buffer) throws IOException {}
}
