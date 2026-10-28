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

package org.apache.celeborn.common.util

import org.apache.celeborn.CelebornFunSuite

class ThreadUtilsSuite extends CelebornFunSuite {

  test("ThreadStackTrace.toString includes blockedByThreadId") {
    val lock = "java.lang.Object@20ad9418"
    val threadStackTrace = ThreadStackTrace(
      threadId = 30L,
      threadName = "blocked-thread",
      threadState = Thread.State.BLOCKED,
      stackTrace = StackTrace(Seq("Foo.bar(Foo.scala:10)\n")),
      blockedByThreadId = Some(29L),
      blockedByLock = lock,
      holdingLocks = Seq.empty,
      synchronizers = Seq.empty,
      monitors = Seq.empty,
      lockName = Some(lock),
      lockOwnerName = Some("holder-thread"),
      suspended = false,
      inNative = false)
    val expected =
      s""""blocked-thread" Id=30 BLOCKED on $lock owned by "holder-thread" Id=29"""

    assert(threadStackTrace.toString.startsWith(expected + "\n"))
  }

  test("ThreadStackTrace.toString without lock owner") {
    val threadStackTrace = ThreadStackTrace(
      threadId = 1L,
      threadName = "running-thread",
      threadState = Thread.State.RUNNABLE,
      stackTrace = StackTrace(Seq("Foo.bar(Foo.scala:10)\n")),
      blockedByThreadId = None,
      blockedByLock = "",
      holdingLocks = Seq.empty,
      synchronizers = Seq.empty,
      monitors = Seq.empty,
      lockName = None,
      lockOwnerName = None,
      suspended = false,
      inNative = false)

    assert(threadStackTrace.toString.startsWith("\"running-thread\" Id=1 RUNNABLE\n"))
  }
}
