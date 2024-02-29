package org.apache.celeborn.server.common.http

import org.apache.commons.lang3.SystemUtils
import org.eclipse.jetty.server.{Handler, HttpConfiguration, HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.server.handler.{ContextHandlerCollection, ErrorHandler}
import org.eclipse.jetty.util.component.LifeCycle
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}

private[celeborn] case class JettyServer(
    server: Server,
    connector: ServerConnector,
    rootHandler: ContextHandlerCollection) {

  def start(): Unit = synchronized {
    try {
      server.start()
      connector.start()
      server.addConnector(connector)
    } catch {
      case e: Exception =>
        stop()
        throw e
    }
  }

  def stop(): Unit = synchronized {
    server.stop()
    connector.stop()
    server.getThreadPool match {
      case lifeCycle: LifeCycle => lifeCycle.stop()
      case _ =>
    }
  }
  def getServerUri: String = connector.getHost + ":" + connector.getLocalPort

  def addHandler(handler: Handler): Unit = synchronized {
    rootHandler.addHandler(handler)
    if (!handler.isStarted) handler.start()
  }

  def addStaticHandler(
      resourceBase: String,
      contextPath: String): Unit = {
    addHandler(JettyUtils.createStaticHandler(resourceBase, contextPath))
  }

  def addRedirectHandler(
      src: String,
      dest: String): Unit = {
    addHandler(JettyUtils.createRedirectHandler(src, dest))
  }

  def getState: String = server.getState
}

object JettyServer {

  def apply(name: String, host: String, port: Int, poolSize: Int): JettyServer = {
    val pool = new QueuedThreadPool(poolSize)
    pool.setName(name)
    pool.setDaemon(true)
    val server = new Server(pool)

    val errorHandler = new ErrorHandler()
    errorHandler.setShowStacks(true)
    errorHandler.setServer(server)
    server.addBean(errorHandler)

    val collection = new ContextHandlerCollection
    server.setHandler(collection)

    val serverExecutor = new ScheduledExecutorScheduler(s"$name-JettyScheduler", true)
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

    new JettyServer(server, connector, collection)
  }
}
