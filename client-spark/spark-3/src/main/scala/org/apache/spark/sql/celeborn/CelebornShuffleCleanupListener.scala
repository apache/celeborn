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
import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.{AdaptiveExecutionContext, AdaptiveSparkPlanExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

import org.apache.celeborn.client.LifecycleManager

/**
 * Unregisters the shuffles of a SQL execution once it completes, so that long-running
 * applications (ThriftServer, Kyuubi, spark-sql, spark-connect-server) do not pile up
 * shuffle data while waiting for driver GC. Stage rerun must be enabled so that a
 * re-executed DataFrame can regenerate its cleaned shuffles. Shuffles of failed or
 * cancelled queries (or stages) that were never materialized are left to GC cleanup.
 * Stages that AQE materializes and then drops from the final plan are covered by the
 * per-query stage cache when spark.sql.exchange.reuse is enabled, and by the shuffle ids
 * that Spark 4 records on materialization when reuse is disabled. Spark 3 has no such
 * collection, so on Spark 3 those stages are still left to GC cleanup.
 *
 * AQE can also drop a stage while its job is still running: the query completes as soon as
 * the remaining stages are materialized, and the dropped stage materializes afterwards. Its
 * shuffle is registered before the query ends but neither reachable from the executed plan
 * nor recorded yet, so it cannot be collected at query end. The listener tracks the shuffle
 * stages of running Spark jobs and unregisters such a shuffle when its job ends, provided the
 * query that submitted it has already completed successfully.
 */
class CelebornShuffleCleanupListener(lifecycleManager: LifecycleManager)
  extends SparkListener with Logging {

  import CelebornShuffleCleanupListener._

  // Jobs that are still running, keyed by Spark job id. Only jobs that carry a SQL execution id
  // and run at least one shuffle stage are tracked.
  private val runningJobs = new mutable.HashMap[Int, RunningJob]()

  // Shuffle ids of jobs that ended while their SQL execution was still running. The query-end
  // cleanup unions them with the ids collected from the plan, which covers a dropped stage whose
  // job ends before the query but whose materialization callback (and so its shuffle id) is only
  // recorded afterwards.
  private val finishedJobShuffles = new mutable.HashMap[Long, mutable.Set[Int]]()

  // SQL executions that already completed, in completion order, together with the driver
  // components used to unregister the shuffles of their late stages. A failed execution maps to
  // None: its shuffles are left to GC cleanup, like for a query that failed before this listener
  // could clean it up.
  private val finishedExecutions =
    new java.util.LinkedHashMap[Long, Option[ShuffleDriverComponents]]()

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

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    try {
      trackRunningJob(jobStart)
    } catch {
      case NonFatal(t) =>
        logWarning(s"Failed to track the shuffle stages of job ${jobStart.jobId}.", t)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    try {
      cleanupLateStageShuffles(jobEnd.jobId)
    } catch {
      case NonFatal(t) =>
        logWarning(s"Failed to cleanup the shuffles of job ${jobEnd.jobId}.", t)
    }
  }

  private def trackRunningJob(start: SparkListenerJobStart): Unit = {
    val executionId = Option(start.properties)
      .flatMap(properties => Option(properties.getProperty(SQLExecution.EXECUTION_ID_KEY)))
      .flatMap(id => Try(id.toLong).toOption)
    val shuffleIds = start.stageInfos.flatMap(_.shuffleDepId).distinct
    if (shuffleIds.nonEmpty) {
      executionId.foreach(id => runningJobs.put(start.jobId, RunningJob(id, shuffleIds)))
    }
  }

  private def cleanupLateStageShuffles(jobId: Int): Unit = {
    runningJobs.remove(jobId).foreach { job =>
      val state = finishedExecutions.get(job.executionId)
      if (state == null) {
        // The query is still running; its query-end cleanup picks these ids up when it ends.
        finishedJobShuffles.getOrElseUpdate(job.executionId, mutable.Set.empty[Int]) ++=
          job.shuffleIds
      } else {
        state.foreach { cleaner =>
          // The query completed before this stage did. The stage was dropped from the final
          // plan while it was still materializing, so its shuffle was registered too late to be
          // collected at query end. All of the stage's map tasks have finished by now.
          val shuffleIds = registeredShuffleIds(job.shuffleIds)
          if (shuffleIds.nonEmpty) {
            logInfo(
              s"Cleaning up shuffles [${shuffleIds.mkString(", ")}] of job $jobId, whose " +
                s"SQL execution ${job.executionId} already completed.")
            shuffleIds.foreach(shuffleId => cleaner.removeShuffle(shuffleId, false))
          }
        }
      }
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
          // Spark 4 records the shuffle ids of successfully materialized stages in the
          // per-query AdaptiveExecutionContext, independently of spark.sql.exchange.reuse,
          // so stages that AQE later drops from the final plan are still covered. Spark 3
          // has no such collection; the reflective lookup then returns None.
          shuffleIds ++= recordedShuffleIds(adaptivePlan)
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
        // because inAdaptivePlan stays true. A stage that AQE materializes and then drops
        // from the final plan is not in this tree; on Spark 4 recordedShuffleIds covers it,
        // on Spark 3 its shuffle is left to GC cleanup. Matched before the generic
        // QueryStageExec case below.
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
      rememberExecution(end.executionId, None)
      finishedJobShuffles.remove(end.executionId)
      return
    }

    val queryFailed = end.executionFailure.isDefined
    val cleaner = qe.sparkSession.sparkContext.shuffleDriverComponents
    // Remember the execution before collecting the ids below, so that a job that ends after
    // this point is recognized as a late stage of an already completed query.
    rememberExecution(end.executionId, if (queryFailed) None else Some(cleaner))

    // If planning itself failed, qe.executedPlan re-triggers planning and re-throws;
    // that is caught as NonFatal by the caller.
    val planShuffleIds = extractShuffleIds(qe.executedPlan, queryFailed)
    // Shuffle ids of stages whose jobs ended before this query. They are usually collected from
    // the plan as well, but a stage that AQE dropped from the final plan may have finished its
    // job before the query ended and still be recorded only afterwards.
    val jobShuffleIds = finishedJobShuffles.remove(end.executionId)
      .map(_.toSeq)
      .getOrElse(Seq.empty)
    // Shuffles of a failed query are left to GC cleanup, even if their stages ran. The ids
    // collected from the plan above still cover the materialized stages of a failed AQE query.
    val shuffleIds = registeredShuffleIds(
      if (queryFailed) planShuffleIds else planShuffleIds ++ jobShuffleIds)

    if (shuffleIds.nonEmpty) {
      logInfo(
        s"Cleaning up shuffles [${shuffleIds.mkString(", ")}] on completion of " +
          s"SQL execution ${end.executionId}.")
      shuffleIds.foreach(shuffleId => cleaner.removeShuffle(shuffleId, false))
    }
  }

  private def registeredShuffleIds(shuffleIds: Seq[Int]): Seq[Int] = {
    shuffleIds.distinct.filter(lifecycleManager.isAppShuffleRegistered(_))
  }

  private def rememberExecution(
      executionId: Long,
      cleaner: Option[ShuffleDriverComponents]): Unit = {
    finishedExecutions.put(executionId, cleaner)
    while (finishedExecutions.size() > MaxRememberedExecutions) {
      val oldest = finishedExecutions.keySet().iterator()
      val evicted = oldest.next()
      oldest.remove()
      finishedJobShuffles.remove(evicted)
    }
  }
}

private object CelebornShuffleCleanupListener {

  // Number of completed SQL executions tracked for cleaning up shuffle stages that are still
  // running when their query ends. Bounds the memory held by the listener in long-running
  // applications while comfortably covering the time a dropped stage can take to finish.
  private val MaxRememberedExecutions = 1024

  // A Spark job that runs the shuffle stage(s) of a SQL execution.
  private case class RunningJob(executionId: Long, shuffleIds: Seq[Int])

  // Spark 4 records the shuffle ids of successfully materialized stages in the per-query
  // AdaptiveExecutionContext (from AdaptiveSparkPlanExec's materialization callback), which
  // is independent of spark.sql.exchange.reuse. Spark 3 has no such collection, and this
  // module also compiles against Spark 3.x, so the accessor is resolved reflectively.
  private val recordedShuffleIdsMethod: Option[Method] =
    Try(classOf[AdaptiveExecutionContext].getMethod("shuffleIds")).toOption

  private def recordedShuffleIds(adaptivePlan: AdaptiveSparkPlanExec): Seq[Int] =
    recordedShuffleIdsMethod.toSeq.flatMap { method =>
      Try(method.invoke(adaptivePlan.context)).toOption
        .collect { case ids: JMap[_, _] =>
          ids.keySet().asScala.map(_.asInstanceOf[Int]).toSeq
        }
        .getOrElse(Seq.empty)
    }

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
