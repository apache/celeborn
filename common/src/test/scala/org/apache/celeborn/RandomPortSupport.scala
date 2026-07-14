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

package org.apache.celeborn

import java.io.IOException
import java.net.{InetSocketAddress, Socket}

import org.apache.celeborn.common.util.Utils

/**
 * Shared random-port picker for cluster-setup test traits. Draws ports below the ephemeral floor
 * (via [[Utils.selectRandomPort]]) and remembers the ports it has handed out so repeated calls
 * within a suite do not collide, retrying if a candidate is already used or currently bound.
 */
trait RandomPortSupport {
  val usedPorts = new java.util.HashSet[Integer]()

  def portBounded(port: Int): Boolean = {
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress("localhost", port), 100)
      true
    } catch {
      case _: IOException => false
    } finally {
      socket.close()
    }
  }

  def selectRandomPort(): Int = synchronized {
    val port = Utils.selectRandomPort()
    val portUsed = usedPorts.contains(port) || portBounded(port)
    usedPorts.add(port)
    if (portUsed) {
      selectRandomPort()
    } else {
      port
    }
  }
}
