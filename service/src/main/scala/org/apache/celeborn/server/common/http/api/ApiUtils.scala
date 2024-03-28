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

import java.lang.management.{LockInfo, ManagementFactory, MonitorInfo, ThreadInfo}

object ApiUtils {
  def getThreadDump(): Seq[ThreadStackTrace] = {
    ManagementFactory.getThreadMXBean.dumpAllThreads(true, true).filter(_ != null).map(
      threadInfoToThreadStackTrace)
  }

  /** Copied from Spark. */
  private def threadInfoToThreadStackTrace(threadInfo: ThreadInfo): ThreadStackTrace = {
    val threadState = threadInfo.getThreadState
    val monitors = threadInfo.getLockedMonitors.map(m => m.getLockedStackDepth -> m.toString).toMap
    val stackTrace = StackTrace(threadInfo.getStackTrace.zipWithIndex.map { case (frame, idx) =>
      val locked =
        if (idx == 0 && threadInfo.getLockInfo != null) {
          threadState match {
            case Thread.State.BLOCKED =>
              s"\t-  blocked on ${threadInfo.getLockInfo}\n"
            case Thread.State.WAITING | Thread.State.TIMED_WAITING =>
              s"\t-  waiting on ${threadInfo.getLockInfo}\n"
            case _ => ""
          }
        } else ""
      val locking = monitors.get(idx).map(mi => s"\t-  locked $mi\n").getOrElse("")
      s"${frame.toString}\n$locked$locking"
    })

    val synchronizers = threadInfo.getLockedSynchronizers.map(_.toString)
    val monitorStrs = monitors.values.toSeq
    ThreadStackTrace(
      threadInfo.getThreadId,
      threadInfo.getThreadName,
      threadState,
      stackTrace,
      if (threadInfo.getLockOwnerId < 0) None else Some(threadInfo.getLockOwnerId),
      Option(threadInfo.getLockInfo).map(lockString).getOrElse(""),
      synchronizers,
      monitorStrs,
      Option(threadInfo.getLockName),
      Option(threadInfo.getLockOwnerName),
      threadInfo.isSuspended,
      threadInfo.isInNative)
  }

  private def lockString(lock: LockInfo): String = {
    lock match {
      case monitor: MonitorInfo => s"Monitor(${monitor.toString})"
      case _ => s"Lock(${lock.toString})"
    }
  }
}
