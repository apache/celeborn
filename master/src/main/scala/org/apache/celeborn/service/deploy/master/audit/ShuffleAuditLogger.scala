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

package org.apache.celeborn.service.deploy.master.audit

import org.apache.celeborn.common.internal.Logging

object ShuffleAuditLogger extends Logging {
  final private val AUDIT_BUFFER = new ThreadLocal[StringBuilder]() {
    override protected def initialValue: StringBuilder = new StringBuilder()
  }

  def audit(shuffleKey: String, op: String, labels: Seq[String] = Seq.empty): Unit = {
    val sb = AUDIT_BUFFER.get()
    sb.setLength(0)
    sb.append(s"shuffleKey=$shuffleKey").append("\t")
    sb.append(s"op=$op")
    if (labels.nonEmpty) sb.append(labels.mkString("\t", "\t", ""))
    logInfo(sb.toString())
  }

  def batchAudit(shuffleKeys: String, op: String, labels: Seq[String] = Seq.empty): Unit = {
    val sb = AUDIT_BUFFER.get()
    sb.setLength(0)
    sb.append(s"shuffleKeys=$shuffleKeys").append("\t")
    sb.append(s"op=$op")
    if (labels.nonEmpty) sb.append(labels.mkString("\t", "\t", ""))
    logInfo(sb.toString())
  }
}
