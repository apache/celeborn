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

  // Note: this runs on the listener-bus thread, so it must never trigger actual computation.
  // ShuffleExchangeExec.shuffleDependency is a lazy val whose evaluation executes the child
  // plan and registers a brand new shuffle with the shuffle manager, so it may only be read
  // from exchanges that are already materialized.
  private def collectShuffleIds(plan: SparkPlan, queryFailed: Boolean): Seq[Int] = {
    val shuffleIds = ArrayBuffer.empty[Int]

    // inAdaptivePlan: whether we are inside an AdaptiveSparkPlanExec subtree without having
    // crossed a materialized shuffle stage. Raw exchanges there may never have been executed
    // (e.g. an AQE query that failed or was cancelled before creating that stage), so their
    // shuffleDependency must not be read.
    def visit(p: SparkPlan, inAdaptivePlan: Boolean): Unit = p match {
      case adaptivePlan: AdaptiveSparkPlanExec =>
        visit(adaptivePlan.executedPlan, inAdaptivePlan = true)

      case shuffleStage: ShuffleQueryStageExec =>
        // Only materialized stages are safe to inspect: their exchange's lazy
        // shuffleDependency is already computed. A materialized stage also implies all
        // query stages nested in its plan subtree are materialized, so keep descending.
        if (shuffleStage.isMaterialized) {
          shuffleStage.shuffle match {
            case exchange: ShuffleExchangeExec =>
              shuffleIds += exchange.shuffleDependency.shuffleId
            // Non-vanilla ShuffleExchangeLike implementations (e.g. Gluten's columnar
            // exchange) are not supported.
            case _ =>
          }
          visit(shuffleStage.plan, inAdaptivePlan = false)
        }

      case stage: QueryStageExec =>
        // Spark 4 wraps the final AQE plan in a ResultQueryStageExec leaf, so descend to
        // reach the shuffle stages referenced by it. BroadcastQueryStageExec and
        // TableCacheQueryStageExec (whose plan is an InMemoryTableScanExec) contain no
        // shuffles, descending into them is harmless.
        visit(stage.plan, inAdaptivePlan)

      case reused: ReusedExchangeExec =>
        // The child of a ReusedExchangeExec is the exchange instance being reused.
        visit(reused.child, inAdaptivePlan)

      case exchange: ShuffleExchangeExec =>
        // Non-AQE plan (or below a materialized AQE stage). For a successful query every
        // exchange has been executed, so its shuffleDependency is already computed. For a
        // failed query an exchange here may never have materialized; reading its lazy
        // shuffleDependency then builds the child RDD lineage on this listener thread,
        // and for RangePartitioning would even launch a sampling job, so those exchanges
        // are skipped (their shuffles, if any, fall back to the regular GC-driven
        // cleanup).
        if (!inAdaptivePlan &&
          (!queryFailed || !exchange.outputPartitioning.isInstanceOf[RangePartitioning])) {
          shuffleIds += exchange.shuffleDependency.shuffleId
        }
        // Earlier query stages sit below this exchange in the tree; keep descending.
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

    // Note: if planning itself failed, accessing qe.executedPlan re-triggers planning and
    // re-throws the planning failure; that exception is caught as NonFatal by the caller.
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
