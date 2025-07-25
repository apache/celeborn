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

package org.apache.celeborn.common.util

import java.util.concurrent._
import java.util.concurrent.{ForkJoinPool => SForkJoinPool, ForkJoinWorkerThread => SForkJoinWorkerThread}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.{Awaitable, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.source.ThreadPoolSource

object ThreadUtils {

  private val sameThreadExecutionContext =
    ExecutionContext.fromExecutorService(sameThreadExecutorService())

  // Inspired by Guava MoreExecutors.sameThreadExecutor; inlined and converted
  // to Scala here to avoid Guava version issues
  def sameThreadExecutorService(): ExecutorService = new AbstractExecutorService {
    private val lock = new ReentrantLock()
    private val termination = lock.newCondition()
    private var runningTasks = 0
    private var serviceIsShutdown = false

    override def shutdown(): Unit = {
      lock.lock()
      try {
        serviceIsShutdown = true
      } finally {
        lock.unlock()
      }
    }

    override def shutdownNow(): java.util.List[Runnable] = {
      shutdown()
      java.util.Collections.emptyList()
    }

    override def isShutdown: Boolean = {
      lock.lock()
      try {
        serviceIsShutdown
      } finally {
        lock.unlock()
      }
    }

    override def isTerminated: Boolean = {
      lock.lock()
      try {
        serviceIsShutdown && runningTasks == 0
      } finally {
        lock.unlock()
      }
    }

    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
      var nanos = unit.toNanos(timeout)
      lock.lock()
      try {
        while (nanos > 0 && !isTerminated()) {
          nanos = termination.awaitNanos(nanos)
        }
        isTerminated()
      } finally {
        lock.unlock()
      }
    }

    override def execute(command: Runnable): Unit = {
      lock.lock()
      try {
        if (isShutdown()) throw new RejectedExecutionException("Executor already shutdown")
        runningTasks += 1
      } finally {
        lock.unlock()
      }
      try {
        command.run()
      } finally {
        lock.lock()
        try {
          runningTasks -= 1
          if (isTerminated()) termination.signalAll()
        } finally {
          lock.unlock()
        }
      }
    }
  }

  /**
   * An `ExecutionContextExecutor` that runs each task in the thread that invokes `execute/submit`.
   * The caller should make sure the tasks running in this `ExecutionContextExecutor` are short and
   * never block.
   */
  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext

  /**
   * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
   */
  def namedThreadFactory(threadNamePrefix: String): ThreadFactory = {
    namedThreadFactory(threadNamePrefix, daemon = true)
  }

  /**
   * Create a thread factory that generates threads with a specified name prefix and daemon setting.
   */
  def namedThreadFactory(threadNamePrefix: String, daemon: Boolean): ThreadFactory = {
    new ThreadFactoryBuilder()
      .setDaemon(daemon)
      .setNameFormat(s"$threadNamePrefix-%d")
      .setUncaughtExceptionHandler(new ThreadExceptionHandler(threadNamePrefix))
      .build()
  }

  /**
   * Create a thread factory that names threads with thread name and also sets the threads to daemon.
   */
  def namedSingleThreadFactory(threadName: String): ThreadFactory = {
    new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat(threadName)
      .setUncaughtExceptionHandler(new ThreadExceptionHandler(threadName))
      .build()
  }

  /**
   * Wrapper over newCachedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadPool =
      Executors.newCachedThreadPool(namedThreadFactory(prefix)).asInstanceOf[ThreadPoolExecutor]
    ThreadPoolSource.registerSource(prefix, threadPool)
    threadPool
  }

  /**
   * Create a cached thread pool whose max number of threads is `maxThreadNumber`. Thread names
   * are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(
      prefix: String,
      maxThreadNumber: Int,
      keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    val threadPool = new ThreadPoolExecutor(
      maxThreadNumber, // corePoolSize: the max number of threads to create before queuing the tasks
      maxThreadNumber, // maximumPoolSize: because we use LinkedBlockingDeque, this one is not used
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      namedThreadFactory(prefix))
    threadPool.allowCoreThreadTimeOut(true)
    ThreadPoolSource.registerSource(prefix, threadPool)
    threadPool
  }

  /**
   * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    newFixedThreadPool(nThreads, prefix, daemon = true)
  }

  /**
   * Wrapper over newFixedThreadPool with daemon setting. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newFixedThreadPool(nThreads: Int, prefix: String, daemon: Boolean): ThreadPoolExecutor = {
    val threadPool = Executors.newFixedThreadPool(nThreads, namedThreadFactory(prefix, daemon))
      .asInstanceOf[ThreadPoolExecutor]
    ThreadPoolSource.registerSource(prefix, threadPool)
    threadPool
  }

  /**
   * Wrapper over newSingleThreadExecutor.
   */
  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    Executors.newSingleThreadExecutor(namedSingleThreadFactory(threadName))
  }

  /**
   * Wrapper over ScheduledThreadPoolExecutor.
   */
  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val executor = new ScheduledThreadPoolExecutor(1, namedSingleThreadFactory(threadName))
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  /**
   * Wrapper over ScheduledThreadPoolExecutor.
   */
  def newDaemonThreadPoolScheduledExecutor(
      threadNamePrefix: String,
      numThreads: Int): ScheduledExecutorService = {
    val executor = new ScheduledThreadPoolExecutor(numThreads, namedThreadFactory(threadNamePrefix))
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  /**
   * Run a piece of code in a new thread and return the result. Exception in the new thread is
   * thrown in the caller thread with an adjusted stack trace that removes references to this
   * method for clarity. The exception stack traces will be like the following
   *
   * SomeException: exception-message
   *   at CallerClass.body-method (sourcefile.scala)
   *   at ... run in separate thread using org.apache.spark.util.ThreadUtils ... ()
   *   at CallerClass.caller-method (sourcefile.scala)
   *   ...
   */
  def runInNewThread[T](
      threadName: String,
      isDaemon: Boolean = true)(body: => T): T = {
    @volatile var exception: Option[Throwable] = None
    @volatile var result: T = null.asInstanceOf[T]

    val thread = new Thread(threadName) {
      override def run(): Unit = {
        try {
          result = body
        } catch {
          case NonFatal(e) =>
            exception = Some(e)
        }
      }
    }
    thread.setDaemon(isDaemon)
    thread.start()
    thread.join()

    exception match {
      case Some(realException) =>
        // Remove the part of the stack that shows method calls into this helper method
        // This means drop everything from the top until the stack element
        // ThreadUtils.runInNewThread(), and then drop that as well (hence the `drop(1)`).
        val baseStackTrace = Thread.currentThread().getStackTrace().dropWhile(
          !_.getClassName.contains(this.getClass.getSimpleName)).drop(1)

        // Remove the part of the new thread stack that shows methods call from this helper method
        val extraStackTrace = realException.getStackTrace.takeWhile(
          !_.getClassName.contains(this.getClass.getSimpleName))

        // Combine the two stack traces, with a place holder just specifying that there
        // was a helper method used, without any further details of the helper
        val placeHolderStackElem = new StackTraceElement(
          s"... run in separate thread using ${ThreadUtils.getClass.getName.stripSuffix("$")} ..",
          " ",
          "",
          -1)
        val finalStackTrace = extraStackTrace ++ Seq(placeHolderStackElem) ++ baseStackTrace

        // Update the stack trace and rethrow the exception in the caller thread
        realException.setStackTrace(finalStackTrace)
        throw realException
      case None =>
        result
    }
  }

  /**
   * Construct a new Scala ForkJoinPool with a specified max parallelism and name prefix.
   */
  def newForkJoinPool(prefix: String, maxThreadNumber: Int): SForkJoinPool = {
    // Custom factory to set thread names
    val factory = new SForkJoinPool.ForkJoinWorkerThreadFactory {
      override def newThread(pool: SForkJoinPool) =
        new SForkJoinWorkerThread(pool) {
          setName(prefix + "-" + super.getName)
        }
    }
    new SForkJoinPool(
      maxThreadNumber,
      factory,
      null, // handler
      false // asyncMode
    )
  }

  // scalastyle:off awaitresult
  /**
   * Preferred alternative to `Await.result()`.
   *
   * This method wraps and re-throws any exceptions thrown by the underlying `Await` call, ensuring
   * that this thread's stack trace appears in logs.
   *
   * In addition, it calls `Awaitable.result` directly to avoid using `ForkJoinPool`'s
   * `BlockingContext`. Codes running in the user's thread may be in a thread of Scala ForkJoinPool.
   * As concurrent executions in ForkJoinPool may see some [[ThreadLocal]] value unexpectedly, this
   * method basically prevents ForkJoinPool from running other tasks in the current waiting thread.
   * In general, we should use this method because many places in Spark use [[ThreadLocal]] and it's
   * hard to debug when [[ThreadLocal]]s leak to other tasks.
   */
  @throws(classOf[CelebornException])
  def awaitResult[T](awaitable: Awaitable[T], atMost: Duration): T = {
    try {
      // `awaitPermission` is not actually used anywhere so it's safe to pass in null here.
      // See SPARK-13747.
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.result(atMost)(awaitPermission)
    } catch {
      // TimeoutException is thrown in the current thread, so not need to warp the exception.
      case NonFatal(t) if !t.isInstanceOf[TimeoutException] =>
        throw new CelebornException("Exception thrown in awaitResult: ", t)
      case e: Throwable =>
        throw e
    }
  }
  // scalastyle:on awaitresult

  // scalastyle:off awaitready
  /**
   * Preferred alternative to `Await.ready()`.
   *
   * @see [[awaitResult]]
   */
  @throws(classOf[CelebornException])
  def awaitReady[T](awaitable: Awaitable[T], atMost: Duration): awaitable.type = {
    try {
      // `awaitPermission` is not actually used anywhere so it's safe to pass in null here.
      // See SPARK-13747.
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.ready(atMost)(awaitPermission)
    } catch {
      // TimeoutException is thrown in the current thread, so not need to warp the exception.
      case NonFatal(t) if !t.isInstanceOf[TimeoutException] =>
        throw new CelebornException("Exception thrown in awaitResult: ", t)
    }
  }
  // scalastyle:on awaitready

  def shutdown(executor: ExecutorService): Unit = {
    shutdown(executor, 800.millis)
  }

  def shutdown(
      executor: ExecutorService,
      gracePeriod: Duration = FiniteDuration(30, TimeUnit.SECONDS)): Unit = {
    if (executor == null) {
      return
    }
    try {
      executor.shutdown()
      if (!executor.awaitTermination(gracePeriod.toMillis, TimeUnit.MILLISECONDS)) {
        executor.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        executor.shutdownNow()
    }
  }

  /**
   * Transforms input collection by applying the given function to each element in parallel fashion.
   * Comparing to the map() method of Scala parallel collections, this method can be interrupted
   * at any time. This is useful on canceling of task execution, for example.
   *
   * @param in - the input collection which should be transformed in parallel.
   * @param prefix - the prefix assigned to the underlying thread pool.
   * @param maxThreads - maximum number of thread can be created during execution.
   * @param f - the lambda function will be applied to each element of `in`.
   * @tparam I - the type of elements in the input collection.
   * @tparam O - the type of elements in resulted collection.
   * @return new collection in which each element was given from the input collection `in` by
   *         applying the lambda function `f`.
   */
  def parmap[I, O](in: Iterable[I], prefix: String, maxThreads: Int)(f: I => O): Iterable[O] = {
    val pool = newForkJoinPool(prefix, maxThreads)
    try {
      implicit val ec = ExecutionContext.fromExecutor(pool)

      val futures = in.map(x => Future(f(x)))
      val futureSeq = Future.sequence(futures)

      awaitResult(futureSeq, Duration.Inf)
    } finally {
      pool.shutdownNow()
    }
  }

  def newThread(runnable: Runnable, name: String): Thread = {
    val thread = new Thread(runnable, name)
    thread.setUncaughtExceptionHandler(new ThreadExceptionHandler(name))
    thread
  }

  def newDaemonThread(runnable: Runnable, name: String): Thread = {
    val thread = newThread(runnable, name)
    thread.setDaemon(true)
    thread
  }
}

class ThreadExceptionHandler(executorService: String)
  extends Thread.UncaughtExceptionHandler with Logging {

  override def uncaughtException(t: Thread, e: Throwable): Unit =
    logError(s"Uncaught exception in executor service $executorService, thread $t", e)
}

/**
 * Note: code was initially copied from Apache Spark(v3.5.1).
 */
case class StackTrace(elems: Seq[String]) {
  override def toString: String = elems.mkString
}

/**
 * Note: code was initially copied from Apache Spark(v3.5.1).
 */
case class ThreadStackTrace(
    threadId: Long,
    threadName: String,
    threadState: Thread.State,
    stackTrace: StackTrace,
    blockedByThreadId: Option[Long],
    blockedByLock: String,
    holdingLocks: Seq[String],
    synchronizers: Seq[String],
    monitors: Seq[String],
    lockName: Option[String],
    lockOwnerName: Option[String],
    suspended: Boolean,
    inNative: Boolean) {

  /**
   * Returns a string representation of this thread stack trace w.r.t java.lang.management.ThreadInfo(JDK 8)'s toString.
   *
   * TODO(SPARK-44896): Also considering adding information os_prio, cpu, elapsed, tid, nid, etc., from the jstack tool
   */
  override def toString: String = {
    val sb = new StringBuilder(
      s""""$threadName" Id=$threadId $threadState""")
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
