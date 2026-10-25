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

import java.lang.reflect.Method
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

import org.apache.celeborn.client.LifecycleManager

/**
 * Unregisters the shuffles of a SQL execution once it completes, so that long-running
 * applications (ThriftServer, Kyuubi, spark-sql, spark-connect-server) do not pile up
 * shuffle data while waiting for driver GC. Stage rerun must be enabled so that a
 * re-executed DataFrame can regenerate its cleaned shuffles. Shuffles of failed or
 * cancelled queries (or stages) that were never materialized are left to GC cleanup.
 */
class CelebornShuffleCleanupListener(lifecycleManager: LifecycleManager)
  extends SparkListener with Logging {

  import CelebornShuffleCleanupListener._

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
    collectShuffleIds(plan, queryFailed).distinct
  }

  // Runs on the listener-bus thread, so it must never trigger computation. Reading the lazy
  // ShuffleExchangeExec.shuffleDependency of an exchange that never executed would run its
  // child plan (and could even launch jobs, e.g. range-partition sampling) on this thread,
  // so only exchanges/stages known to have executed may be touched.
  private def collectShuffleIds(plan: SparkPlan, queryFailed: Boolean): Seq[Int] = {
    val shuffleIds = ArrayBuffer.empty[Int]

    // inAdaptivePlan: inside an AdaptiveSparkPlanExec subtree, where raw exchanges may be
    // unexecuted; their shuffles are collected from the shared stage cache instead.
    def visit(p: SparkPlan, inAdaptivePlan: Boolean): Unit = {
      p match {
        case adaptivePlan: AdaptiveSparkPlanExec =>
          // The stage cache is shared by the whole query (including subqueries) and holds
          // every stage the query created, even stages dropped from the final plan (e.g.
          // an empty join side) and children of unmaterialized stages. Only materialized
          // stages are read, since their shuffleDependency is already computed.
          adaptivePlan.context.stageCache.values.foreach {
            case stage: ShuffleQueryStageExec if isStageMaterialized(stage) =>
              // Non-vanilla ShuffleExchangeLike (e.g. Gluten's columnar exchange) is ignored.
              stage.shuffle match {
                case exchange: ShuffleExchangeExec =>
                  shuffleIds += exchange.shuffleDependency.shuffleId
                case _ =>
              }
            case _ =>
          }
          // Keep looking for nested AdaptiveSparkPlanExec nodes (e.g. under a
          // DataWritingCommandExec for CTAS, or subquery plans); stages already drained
          // from a shared cache are only re-collected as duplicates, deduplicated later.
          visit(adaptivePlan.executedPlan, inAdaptivePlan = true)

        // The stage cache only fills when spark.sql.exchange.reuse is on; with reuse off
        // the materialized stages are collected from the tree here instead (duplicates
        // are removed by distinct later). Descend into the stage plan to reach nested
        // child stages; the raw exchange of an unmaterialized stage is never touched
        // because inAdaptivePlan stays true. Matched before the generic QueryStageExec
        // case below.
        case stage: ShuffleQueryStageExec =>
          if (isStageMaterialized(stage)) {
            // Non-vanilla ShuffleExchangeLike (e.g. Gluten's columnar exchange) is ignored.
            stage.shuffle match {
              case exchange: ShuffleExchangeExec =>
                shuffleIds += exchange.shuffleDependency.shuffleId
              case _ =>
            }
          }
          visit(stage.plan, inAdaptivePlan = true)

        // Spark 4 wraps the final AQE plan in a ResultQueryStageExec leaf. Other query
        // stages may wrap plan subtrees holding nested adaptive plans.
        case stage: QueryStageExec =>
          visit(stage.plan, inAdaptivePlan)

        case reused: ReusedExchangeExec =>
          visit(reused.child, inAdaptivePlan)

        case exchange: ShuffleExchangeExec =>
          // Only read the id of a raw exchange when the query succeeded outside AQE, in
          // which case the exchange has executed and its lazy shuffleDependency is already
          // computed. For failed queries the shuffles are left to GC cleanup, since reading
          // the lazy val here could execute the child plan on this thread.
          if (!inAdaptivePlan && !queryFailed) {
            shuffleIds += exchange.shuffleDependency.shuffleId
          }
          exchange.children.foreach(visit(_, inAdaptivePlan))

        case other =>
          other.children.foreach(visit(_, inAdaptivePlan))
      }
      // Subqueries live in the expressions of each node. AQE subquery shuffles are already
      // covered by the shared stage cache above; this covers non-AQE subqueries.
      p.subqueries.foreach(visit(_, inAdaptivePlan))
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
      .filter(lifecycleManager.isAppShuffleRegistered(_))

    if (shuffleIds.nonEmpty) {
      logInfo(
        s"Cleaning up shuffles [${shuffleIds.mkString(", ")}] on completion of " +
          s"SQL execution ${end.executionId}.")
      shuffleIds.foreach(shuffleId =>
        qe.sparkSession.sparkContext.shuffleDriverComponents.removeShuffle(shuffleId, false))
    }
  }
}

private object CelebornShuffleCleanupListener {

  // QueryStageExec.isMaterialized only exists since Spark 3.2; on Spark 3.0/3.1 the
  // materialization result is read from resultOption instead (an Option on 3.0, an
  // AtomicReference[Option] on 3.1).
  private val isStageMaterialized: ShuffleQueryStageExec => Boolean = {
    def method(name: String): Option[Method] =
      Try(classOf[QueryStageExec].getMethod(name)).toOption

    method("isMaterialized").map { m => (stage: ShuffleQueryStageExec) =>
      m.invoke(stage).asInstanceOf[Boolean]
    }.getOrElse {
      val resultOption = classOf[QueryStageExec].getMethod("resultOption")
      (stage: ShuffleQueryStageExec) =>
        resultOption.invoke(stage) match {
          case ref: AtomicReference[_] => ref.get().asInstanceOf[Option[_]].isDefined
          case option: Option[_] => option.isDefined
          case _ => false
        }
    }
  }
}
