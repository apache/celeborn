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

package org.apache.celeborn.service.deploy.worker.decommission

import java.util.concurrent.atomic.AtomicInteger

import org.apache.celeborn.common.CelebornConf

class AlwaysDecommissionSignalChecker(conf: CelebornConf) extends DecommissionSignalChecker {
  override def shouldDecommission(): Boolean = true

  override def getReason: String = "test reason"

  override def close(): Unit = AlwaysDecommissionSignalChecker.closeCount.incrementAndGet()
}

object AlwaysDecommissionSignalChecker {
  val closeCount = new AtomicInteger(0)
}

class ThrowingThenTrueDecommissionSignalChecker(
    failures: Int,
    newError: () => Throwable = () => new RuntimeException("checker failure"))
  extends DecommissionSignalChecker {
  private val calls = new AtomicInteger(0)

  override def shouldDecommission(): Boolean = {
    if (calls.incrementAndGet() <= failures) {
      throw newError()
    }
    true
  }
}

class NotADecommissionSignalChecker(conf: CelebornConf)

class NoConfConstructorDecommissionSignalChecker extends DecommissionSignalChecker {
  override def shouldDecommission(): Boolean = false
}

class ConstructorFailingDecommissionSignalChecker(conf: CelebornConf)
  extends DecommissionSignalChecker {
  throw new IllegalStateException("constructor failed")

  override def shouldDecommission(): Boolean = false
}
