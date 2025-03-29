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

package org.apache.celeborn.verifier.protocol

import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.verifier.action.Operation
import org.apache.celeborn.verifier.info.NodeStatus
import org.apache.celeborn.verifier.plan.VerificationPlan

sealed trait Message extends Serializable

class ResponseMessage(val statusCode: StatusCode, val exception: Exception) extends Message

case class RegisterRunner(uid: String, resource: NodeStatus) extends Message

case class RegisterRunnerResponse(
    override val statusCode: StatusCode,
    override val exception: Exception) extends ResponseMessage(statusCode, exception)

case class SubmitPlan(plan: VerificationPlan) extends Message

case class SubmitPlanResponse(
    override val statusCode: StatusCode,
    override val exception: Exception) extends ResponseMessage(statusCode, exception)

case class PausePlan() extends Message

case class PausePlanResponse(override val statusCode: StatusCode, override val exception: Exception)
  extends ResponseMessage(statusCode, exception)

case class ResumePlan() extends Message

case class ResumePlanResponse(
    override val statusCode: StatusCode,
    override val exception: Exception) extends ResponseMessage(statusCode, exception)

case class StopPlan() extends Message

case class StopPlanResponse(override val statusCode: StatusCode, override val exception: Exception)
  extends ResponseMessage(statusCode, exception)

case class QueryStatus() extends Message

case class QueryStatusResponse(
    override val statusCode: StatusCode,
    override val exception: Exception,
    report: String) extends ResponseMessage(statusCode, exception)

case class ExecuteOperation(operation: Operation) extends Message

case class ExecuteOperationResponse(
    override val statusCode: StatusCode,
    override val exception: Exception,
    resource: NodeStatus,
    uid: String) extends ResponseMessage(statusCode, exception)

case class RunnerHeartBeat(uid: String, resource: NodeStatus, diff: NodeStatus) extends Message

case class CheckRunnerTimeout() extends Message
