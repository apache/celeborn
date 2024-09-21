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

package org.apache.celeborn.server.common.http

import scala.util.Try

import org.apache.commons.lang3.SystemUtils
import org.eclipse.jetty.server.{Handler, HttpConfiguration, HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.server.handler.{ContextHandlerCollection, ErrorHandler}
import org.eclipse.jetty.util.component.LifeCycle
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.CelebornExitKind

private[celeborn] case class HttpServer(
    role: String,
    server: Server,
    connector: ServerConnector,
    rootHandler: ContextHandlerCollection) extends Logging {

  @volatile private var isStarted = false

  @throws[Exception]
  def start(): Unit = synchronized {
    try {
      server.start()
      connector.start()
      server.addConnector(connector)
      logInfo(s"$role: HttpServer started on ${connector.getHost}:${connector.getPort}.")
      isStarted = true
    } catch {
      case e: Exception =>
        stopInternal(CelebornExitKind.EXIT_IMMEDIATELY)
        throw e
    }
  }

  def stop(exitCode: Int): Unit = synchronized {
    if (isStarted) {
      stopInternal(exitCode)
    }
  }

  private def stopInternal(exitCode: Int): Unit = {
    if (exitCode == CelebornExitKind.EXIT_IMMEDIATELY) {
      server.setStopTimeout(0)
      connector.setStopTimeout(0)
    }
    val threadPool = server.getThreadPool
    threadPool match {
      case pool: QueuedThreadPool =>
        // avoid Jetty's acceptor thread shrink
        Try(pool.setIdleTimeout(0))
      case _ =>
    }
    logInfo(s"$role: Stopping HttpServer")
    server.stop()
    server.join()
    connector.stop()
    // Stop the ThreadPool if it supports stop() method (through LifeCycle).
    // It is needed because stopping the Server won't stop the ThreadPool it uses.
    threadPool match {
      case lifeCycle: LifeCycle => lifeCycle.stop()
      case _ =>
    }
    logInfo(s"$role: HttpServer stopped.")
    isStarted = false
  }

  def getServerUri: String = connector.getHost + ":" + connector.getLocalPort

  def addHandler(handler: Handler): Unit = synchronized {
    rootHandler.addHandler(handler)
    if (!handler.isStarted) handler.start()
  }

  def addStaticHandler(
      resourceBase: String,
      contextPath: String): Unit = {
    addHandler(HttpUtils.createStaticHandler(resourceBase, contextPath))
  }

  def addRedirectHandler(
      src: String,
      dest: String): Unit = {
    addHandler(HttpUtils.createRedirectHandler(src, dest))
  }

  def getState: String = server.getState
}

object HttpServer extends Logging {

  def apply(
      role: String,
      host: String,
      port: Int,
      poolSize: Int,
      stopTimeout: Long,
      idleTimeout: Long,
      sslEnabled: Boolean,
      keyStorePath: Option[String],
      keyStorePassword: Option[String],
      keyStoreType: Option[String],
      keyStoreAlgorithm: Option[String],
      sslDisallowedProtocols: Seq[String],
      sslIncludeCipherSuites: Seq[String]): HttpServer = {
    val pool = new QueuedThreadPool(math.max(poolSize, 8))
    pool.setName(s"$role-JettyThreadPool")
    pool.setDaemon(true)
    val server = new Server(pool)
    server.setStopTimeout(stopTimeout)

    val errorHandler = new ErrorHandler()
    errorHandler.setShowStacks(true)
    errorHandler.setServer(server)
    server.addBean(errorHandler)

    val collection = new ContextHandlerCollection
    server.setHandler(collection)

    if (sslEnabled) {
      if (keyStorePath.isEmpty) {
        throw new IllegalArgumentException("KeyStorePath is not provided for SSL connection.")
      }
      if (keyStorePassword.isEmpty) {
        throw new IllegalArgumentException("KeyStorePassword is not provided for SSL connection.")
      }

      val sslContextFactory = new SslContextFactory.Server()
      logInfo("HTTP Server SSL: adding excluded protocols: " + sslDisallowedProtocols.mkString(","))
      sslContextFactory.addExcludeProtocols(sslDisallowedProtocols: _*)
      logInfo(s"HTTP Server SSL: SslContextFactory.getExcludeProtocols = ${sslContextFactory.getExcludeProtocols.mkString(",")}")
      logInfo(
        "HTTP Server SSL: adding included cipher suites: " + sslIncludeCipherSuites.mkString(","))
      sslContextFactory.setIncludeCipherSuites(sslIncludeCipherSuites: _*)
      logInfo(s"HTTP Server SSL: SslContextFactory.getIncludeCipherSuites = ${sslContextFactory.getIncludeCipherSuites.mkString(",")}")
    }

    val serverExecutor = new ScheduledExecutorScheduler(s"$role-JettyScheduler", true)
    val httpConf = new HttpConfiguration()
    val connector = new ServerConnector(
      server,
      null,
      serverExecutor,
      null,
      -1,
      -1,
      new HttpConnectionFactory(httpConf))
    connector.setHost(host)
    connector.setPort(port)
    connector.setReuseAddress(!SystemUtils.IS_OS_WINDOWS)
    connector.setAcceptQueueSize(math.min(connector.getAcceptors, 8))
    connector.setStopTimeout(stopTimeout)
    connector.setIdleTimeout(idleTimeout)

    new HttpServer(role, server, connector, collection)
  }
}
