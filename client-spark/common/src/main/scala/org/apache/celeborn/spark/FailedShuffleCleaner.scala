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
package org.apache.celeborn.spark

import java.util
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.shuffle.celeborn.SparkCommonUtils

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.ThreadUtils

private[celeborn] object FailedShuffleCleaner extends Logging {

  private val lifecycleManager = new AtomicReference[LifecycleManager](null)
  // in celeborn ids
  private val shufflesToBeCleaned = new LinkedBlockingQueue[Int]()
  private val cleanedShuffleIds = new mutable.HashSet[Int]

  private val lock = new Object

  private lazy val cleanInterval =
    lifecycleManager.get().conf.clientFetchCleanFailedShuffleIntervalMS

  private def buildRunningStageChecker(): RunningStageManager = {
    val lifecycleMgrRef = lifecycleManager.get()
    if (lifecycleMgrRef != null) {
      val className = lifecycleManager.get().conf.clientFetchCleanFailedShuffleRunningMgrImpl
      val claz = Class.forName(className)
      claz.getDeclaredConstructor().newInstance().asInstanceOf[RunningStageManager]
    } else {
      null
    }
  }

  // for test
  def reset(): Unit = {
    shufflesToBeCleaned.clear()
    cleanedShuffleIds.clear()
    if (cleanerThreadPool != null) {
      cleanerThreadPool.shutdownNow()
      cleanerThreadPool = null
    }
    lifecycleManager.set(null)
  }

  def addShuffleIdToBeCleaned(appShuffleIdentifier: String): Unit = {
    val Array(appShuffleId, _, _) = SparkCommonUtils.decodeAppShuffleIdentifier(
      appShuffleIdentifier)
    lifecycleManager.get().getShuffleIdMapping.get(appShuffleId.toInt).foreach {
      case (_, (celebornShuffleId, _)) => shufflesToBeCleaned.put(celebornShuffleId)
    }
  }

  def setLifecycleManager(ref: LifecycleManager): Unit = {
    val firstSet = lifecycleManager.compareAndSet(null, ref)
    if (firstSet) {
      cleanerThreadPool = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "failedShuffleCleanerThreadPool")
      cleanerThreadPool.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit = {
            try {
              val allShuffleIds = new util.ArrayList[Int]
              shufflesToBeCleaned.drainTo(allShuffleIds)
              allShuffleIds.asScala.foreach { shuffleId =>
                if (!cleanedShuffleIds.contains(shuffleId)) {
                  lifecycleManager.get().unregisterShuffle(shuffleId)
                  logInfo(
                    s"sent unregister shuffle request for shuffle $shuffleId (celeborn shuffle id)")
                  cleanedShuffleIds += shuffleId
                }
              }
            } catch {
              case e: Exception =>
                logError("unexpected exception in cleaner thread", e)
            }
          }
        },
        cleanInterval,
        cleanInterval,
        TimeUnit.MILLISECONDS)
    }
  }

  def removeCleanedShuffleId(celebornShuffleId: Int): Unit = {
    cleanedShuffleIds.remove(celebornShuffleId)
  }

  private var cleanerThreadPool: ScheduledExecutorService = _
}
