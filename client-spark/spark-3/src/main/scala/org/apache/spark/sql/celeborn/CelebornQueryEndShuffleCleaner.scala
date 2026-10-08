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

package org.apache.spark.sql.celeborn

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

import org.apache.celeborn.client.LifecycleManager

/**
 * Unregisters the shuffles of a SQL execution once it completes, so that long-running
 * applications (ThriftServer, Kyuubi, spark-sql, spark-connect-server) do not pile up
 * shuffle data while waiting for driver GC. Stage rerun must be enabled so that a
 * re-executed DataFrame can regenerate its cleaned shuffles.
 */
class CelebornQueryEndShuffleCleaner(lifecycleManager: LifecycleManager)
  extends SparkListener with Logging {

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case end: SparkListenerSQLExecutionEnd =>
        try {
          cleanupShufflesOnQueryEnd(end)
        } catch {
          case NonFatal(t) =>
            logWarning(
              s"Failed to cleanup shuffles on completion of SQL execution ${end.executionId}.",
              t)
        }
      case _ =>
    }
  }

  private def extractShuffleIds(plan: SparkPlan, queryFailed: Boolean): Seq[Int] = {
    (plan +: plan.subqueriesAll).flatMap(collectShuffleIds(_, queryFailed)).distinct
  }

  // Runs on the listener-bus thread, so it must never trigger computation. Reading the lazy
  // ShuffleExchangeExec.shuffleDependency of an exchange that never executed would run its
  // child plan and register a new shuffle, so only materialized exchanges may be touched.
  private def collectShuffleIds(plan: SparkPlan, queryFailed: Boolean): Seq[Int] = {
    val shuffleIds = ArrayBuffer.empty[Int]

    // inAdaptivePlan: inside an AdaptiveSparkPlanExec without having crossed a materialized
    // shuffle stage; raw exchanges there may be unexecuted (failed/cancelled AQE queries).
    def visit(p: SparkPlan, inAdaptivePlan: Boolean): Unit = p match {
      case adaptivePlan: AdaptiveSparkPlanExec =>
        visit(adaptivePlan.executedPlan, inAdaptivePlan = true)

      case shuffleStage: ShuffleQueryStageExec =>
        if (shuffleStage.isMaterialized) {
          // Non-vanilla ShuffleExchangeLike (e.g. Gluten's columnar exchange) is ignored.
          shuffleStage.shuffle match {
            case exchange: ShuffleExchangeExec =>
              shuffleIds += exchange.shuffleDependency.shuffleId
            case _ =>
          }
          // A materialized stage only references materialized child stages; keep descending.
          visit(shuffleStage.plan, inAdaptivePlan = false)
        }

      // Spark 4 wraps the final AQE plan in a ResultQueryStageExec leaf.
      case stage: QueryStageExec =>
        visit(stage.plan, inAdaptivePlan)

      case reused: ReusedExchangeExec =>
        visit(reused.child, inAdaptivePlan)

      case exchange: ShuffleExchangeExec =>
        // In a failed query an exchange here may be unexecuted; RangePartitioning ones are
        // skipped since reading them could launch a sampling job on this thread.
        if (!inAdaptivePlan &&
          (!queryFailed || !exchange.outputPartitioning.isInstanceOf[RangePartitioning])) {
          shuffleIds += exchange.shuffleDependency.shuffleId
        }
        exchange.children.foreach(visit(_, inAdaptivePlan))

      case other =>
        other.children.foreach(visit(_, inAdaptivePlan))
    }

    visit(plan, inAdaptivePlan = false)
    shuffleIds.toSeq
  }

  private def cleanupShufflesOnQueryEnd(end: SparkListenerSQLExecutionEnd): Unit = {
    val qe = end.qe
    if (qe == null) {
      logDebug(
        s"QueryExecution is null in SparkListenerSQLExecutionEnd ${end.executionId}, skipping.")
      return
    }

    // If planning itself failed, qe.executedPlan re-triggers planning and re-throws;
    // that is caught as NonFatal by the caller.
    val shuffleIds = extractShuffleIds(qe.executedPlan, end.executionFailure.isDefined)
      .filter(
        lifecycleManager.isAppShuffleRegistered(_, lifecycleManager.conf.clientStageRerunEnabled))

    if (shuffleIds.nonEmpty) {
      logInfo(
        s"Cleaning up shuffles [${shuffleIds.mkString(", ")}] on completion of " +
          s"SQL execution ${end.executionId}.")
      shuffleIds.foreach(shuffleId =>
        qe.sparkSession.sparkContext.shuffleDriverComponents.removeShuffle(shuffleId, false))
    }
  }
}
