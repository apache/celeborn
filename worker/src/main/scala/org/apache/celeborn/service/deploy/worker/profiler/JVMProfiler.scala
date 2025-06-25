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

package org.apache.celeborn.service.deploy.worker.profiler

import java.io.IOException

import one.profiler.{AsyncProfiler, AsyncProfilerLoader}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils

/**
 * The JVM profiler provides code profiling of worker based on the the async profiler, a low overhead sampling profiler for Java.
 * This allows a worker to capture CPU and memory profiles for worker which can later be analyzed for performance issues.
 * The profiler captures Java Flight Recorder (jfr) files for each worker read by tools including Java Mission Control and Intellij.
 *
 * <p>Note: The profiler writes the jfr files to the worker's working directory in the worker's local file system and the files can grow to be large so it is advisable
 * that the worker machines have adequate storage.
 *
 * <p>Note: code copied from Apache Spark.
 *
 * @param conf Celeborn configuration with jvm profiler config.
 */
class JVMProfiler(conf: CelebornConf) extends Logging {

  private var running = false
  private val enableProfiler = conf.workerJvmProfilerEnabled
  private val profilerOptions = conf.workerJvmProfilerOptions
  private val profilerLocalDir = conf.workerJvmProfilerLocalDir

  private val startcmd = s"start,$profilerOptions,file=$profilerLocalDir/profile.jfr"
  private val stopcmd = s"stop,$profilerOptions,file=$profilerLocalDir/profile.jfr"

  private lazy val extractionDir = Utils.createTempDir(profilerLocalDir, "profiler").toPath

  val profiler: Option[AsyncProfiler] = {
    Option(
      if (enableProfiler && AsyncProfilerLoader.isSupported) {
        logInfo(s"Profiler extraction directory: ${extractionDir.toString}.")
        AsyncProfilerLoader.setExtractionDirectory(extractionDir)
        AsyncProfilerLoader.load()
      } else { null })
  }

  def start(): Unit = {
    if (!running) {
      try {
        profiler.foreach(p => {
          p.execute(startcmd)
          logInfo("JVM profiling started.")
          running = true
        })
      } catch {
        case e @ (_: IllegalArgumentException | _: IllegalStateException | _: IOException) =>
          logError("JVM profiling aborted. Exception occurred in profiler native code: ", e)
        case e: Exception => logWarning("JVM profiling aborted due to exception: ", e)
      }
    }
  }

  /** Stops the profiling and saves output to dfs location. */
  def stop(): Unit = {
    if (running) {
      profiler.foreach(p => {
        p.execute(stopcmd)
        logInfo("JVM profiler stopped.")
        running = false
      })
    }
  }
}
