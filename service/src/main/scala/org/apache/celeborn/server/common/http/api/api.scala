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

package org.apache.celeborn.server.common.http.api

/** Copied from Spark. */
case class ThreadStackTrace(
    threadId: Long,
    threadName: String,
    threadState: Thread.State,
    stackTrace: StackTrace,
    blockedByThreadId: Option[Long],
    blockedByLock: String,
    synchronizers: Seq[String],
    monitors: Seq[String],
    lockName: Option[String],
    lockOwnerName: Option[String],
    suspended: Boolean,
    inNative: Boolean) {

  /**
   * Returns a string representation of this thread stack trace
   * w.r.t java.lang.management.ThreadInfo(JDK 8)'s toString.
   */
  override def toString: String = {
    val sb = new StringBuilder(
      s""""$threadName Id=$threadId $threadState""")
    lockName.foreach(lock => sb.append(s" on $lock"))
    lockOwnerName.foreach {
      owner => sb.append(s"""owned by "$owner"""")
    }
    blockedByThreadId.foreach(id => s" Id=$id")
    if (suspended) sb.append(" (suspended)")
    if (inNative) sb.append(" (in native)")
    sb.append('\n')

    sb.append(stackTrace.elems.map(e => s"\tat $e").mkString)

    if (synchronizers.nonEmpty) {
      sb.append(s"\n\tNumber of locked synchronizers = ${synchronizers.length}\n")
      synchronizers.foreach(sync => sb.append(s"\t- $sync\n"))
    }
    sb.append('\n')
    sb.toString
  }
}

case class StackTrace(elems: Seq[String])
