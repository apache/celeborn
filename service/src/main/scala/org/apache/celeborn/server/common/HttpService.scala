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

package org.apache.celeborn.server.common

import java.util
import javax.servlet.DispatcherType

import scala.collection.JavaConverters._

import org.eclipse.jetty.servlet.FilterHolder

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.server.common.http.HttpServer
import org.apache.celeborn.server.common.http.api.ApiRootResource
import org.apache.celeborn.server.common.http.authentication.{AuthenticationFilter, HttpAuthenticationFactory}
import org.apache.celeborn.server.common.service.config.ConfigLevel

abstract class HttpService extends Service with Logging {
  type HandleResponse = (Boolean, String)

  private var httpServer: HttpServer = _

  def getConf: String = {
    val sb = new StringBuilder
    sb.append("=========================== Configuration ============================\n")
    if (conf.getAll.nonEmpty) {
      val redactedConf = Utils.redact(conf, conf.getAll)
      val maxKeyLength = redactedConf.toMap.keys.map(_.length).max
      redactedConf.sortBy(_._1).foreach { case (key, value) =>
        sb.append(config(key, value, maxKeyLength))
      }
    }
    sb.toString()
  }

  def getDynamicConfigs(
      level: String,
      tenant: String,
      name: String): String = {
    if (configService == null) {
      s"Dynamic configuration is disabled. Please check whether to config `${CelebornConf.DYNAMIC_CONFIG_STORE_BACKEND.key}`."
    } else {
      val sb = new StringBuilder
      sb.append("=========================== Dynamic Configuration ============================\n")
      if (level.isEmpty) {
        sb.append(dynamicConfigs(tenant, name))
      } else {
        sb.append(dynamicConfigs(level, tenant, name))
      }
      sb.toString()
    }
  }

  private def dynamicConfigs(tenant: String, name: String): String = {
    ConfigLevel.values().map { configLevel =>
      dynamicConfigs(configLevel.name(), tenant, name)
    }.mkString("\n")
  }

  private def dynamicConfigs(level: String, tenant: String, name: String): String = {
    val sb = new StringBuilder
    sb.append(
      s"=========================== Level: $level ============================\n")
    if (ConfigLevel.SYSTEM.name().equalsIgnoreCase(level)) {
      sb.append(systemConfigs())
    } else if (ConfigLevel.TENANT.name().equalsIgnoreCase(level)) {
      sb.append(tenantConfigs(tenant))
    } else if (ConfigLevel.TENANT_USER.name().equalsIgnoreCase(level)) {
      sb.append(tenantUserConfigs(tenant, name))
    }
    sb.toString()
  }

  private def systemConfigs(): String = {
    // When setting config level is SYSTEM, returns all system level configs.
    configs(configService.getSystemConfigFromCache.getConfigs)
  }

  private def tenantConfigs(tenant: String): String = {
    // When setting config level is TENANT without tenant id, returns all tenant level configs.
    // When setting config level is TENANT with tenant id, returns only tenant level configs of given tenant id.
    val tenantConfigs =
      if (tenant.isEmpty) {
        configService.listRawTenantConfigsFromCache().asScala
      } else {
        List(configService.getRawTenantConfigFromCache(tenant))
      }
    tenantConfigs.sortBy(_.getTenantId).map { tenantConfig =>
      s"""
         |=========================== Tenant: ${tenantConfig.getTenantId} ============================
         |${configs(tenantConfig.getConfigs)}""".stripMargin
    }.mkString("\n")
  }

  private def tenantUserConfigs(tenant: String, name: String): String = {
    // When setting config level is TENANT_USER without tenant id and user name, returns all tenant user level configs.
    // When setting config level is TENANT_USER with tenant id and user name, returns only tenant user level configs of given tenant id and user name.
    val tenantUserConfigs =
      if (tenant.isEmpty && name.isEmpty) {
        configService.listRawTenantUserConfigsFromCache().asScala
      } else if (tenant.nonEmpty && name.nonEmpty) {
        List(configService.getRawTenantUserConfigFromCache(tenant, name))
      } else {
        List()
      }
    tenantUserConfigs.sortBy(_.getTenantId).map { tenantUserConfig =>
      s"""
         |=========================== Tenant: ${tenantUserConfig.getTenantId}, Name: ${tenantUserConfig.getName} ============================
         |${configs(tenantUserConfig.getConfigs)}""".stripMargin
    }.mkString("\n")
  }

  private def configs(configs: util.Map[String, String]): String = {
    val sb = new StringBuilder
    val configMap = configs.asScala
    if (configMap.nonEmpty) {
      val maxKeyLength = configMap.keys.map(_.length).max
      configMap.toSeq.sortBy(_._1).foreach { case (key, value) =>
        sb.append(config(key, value, maxKeyLength))
      }
    }
    sb.toString()
  }

  private def config(configKey: String, configVal: String, maxKeyLength: Int): String =
    s"${configKey.padTo(maxKeyLength + 10, " ").mkString}$configVal\n"

  def getWorkerInfo: String

  def getThreadDump: String = {
    val sb = new StringBuilder
    sb.append(
      s"========================= ${serviceName.capitalize} ThreadDump ==========================\n")
    sb.append(Utils.getThreadDump().mkString("\n")).append("\n")
    sb.toString()
  }

  def getShuffleList: String

  def getApplicationList: String

  def listTopDiskUseApps: String

  def getMasterGroupInfo: String = throw new UnsupportedOperationException()

  def getLostWorkers: String = throw new UnsupportedOperationException()

  def getShutdownWorkers: String = throw new UnsupportedOperationException()

  def getDecommissionWorkers: String = throw new UnsupportedOperationException()

  def getExcludedWorkers: String = throw new UnsupportedOperationException()

  def getHostnameList: String = throw new UnsupportedOperationException()

  def exclude(addWorkers: Seq[WorkerInfo], removeWorkers: Seq[WorkerInfo]): HandleResponse =
    throw new UnsupportedOperationException()

  def listPartitionLocationInfo: String = throw new UnsupportedOperationException()

  def getUnavailablePeers: String = throw new UnsupportedOperationException()

  def isShutdown: String = throw new UnsupportedOperationException()

  def isRegistered: String = throw new UnsupportedOperationException()

  def isDecommissioning: String = throw new UnsupportedOperationException()

  def exit(exitType: String): String = throw new UnsupportedOperationException()

  def handleWorkerEvent(workerEventType: String, workers: Seq[WorkerInfo]): HandleResponse =
    throw new UnsupportedOperationException()

  def getWorkerEventInfo(): String = throw new UnsupportedOperationException()

  def startHttpServer(): Unit = {
    httpServer = HttpServer(
      serviceName,
      httpHost(),
      httpPort(),
      httpMaxWorkerThreads(),
      httpStopTimeout(),
      httpIdleTimeout())
    httpServer.start()
    startInternal()
    // block until the HTTP server is started, otherwise, we may get
    // the wrong HTTP server port -1
    while (httpServer.getState != "STARTED") {
      logInfo(s"Waiting for $serviceName's HTTP server getting started")
      Thread.sleep(1000)
    }
  }

  private def httpHost(): String = {
    if (conf.bindWildcardAddress) {
      null
    } else {
      serviceName match {
        case Service.MASTER =>
          conf.masterHttpHost
        case Service.WORKER =>
          conf.workerHttpHost
      }
    }
  }

  private def httpPort(): Int = {
    serviceName match {
      case Service.MASTER =>
        conf.masterHttpPort
      case Service.WORKER =>
        conf.workerHttpPort
    }
  }

  private def httpMaxWorkerThreads(): Int = {
    serviceName match {
      case Service.MASTER =>
        conf.masterHttpMaxWorkerThreads
      case Service.WORKER =>
        conf.workerHttpMaxWorkerThreads
    }
  }

  private def httpStopTimeout(): Long = {
    serviceName match {
      case Service.MASTER =>
        conf.masterHttpStopTimeout
      case Service.WORKER =>
        conf.workerHttpStopTimeout
    }
  }

  private def httpIdleTimeout(): Long = {
    serviceName match {
      case Service.MASTER =>
        conf.masterHttpIdleTimeout
      case Service.WORKER =>
        conf.workerHttpIdleTimeout
    }
  }

  def connectionUrl: String = {
    httpServer.getServerUri
  }

  protected def startInternal(): Unit = {
    val contextHandler = ApiRootResource.getServletHandler(this)
    val holder = new FilterHolder(new AuthenticationFilter(conf, serviceName))
    contextHandler.addFilter(holder, "/*", util.EnumSet.allOf(classOf[DispatcherType]))
    httpServer.addHandler(HttpAuthenticationFactory.wrapHandler(contextHandler))

    httpServer.addStaticHandler("META-INF/resources/webjars/swagger-ui/4.9.1/", "/swagger-static/")
    httpServer.addStaticHandler("org/apache/celeborn/swagger", "/swagger")
    httpServer.addRedirectHandler("/help", "/swagger")
    httpServer.addRedirectHandler("/docs", "/swagger")

    if (metricsSystem.running) {
      metricsSystem.getServletContextHandlers.foreach { handler =>
        httpServer.addHandler(handler)
      }
    }
  }

  override def initialize(): Unit = {
    super.initialize()
    startHttpServer()
  }

  override def stop(exitKind: Int): Unit = {
    // may be null when running the unit test
    if (null != httpServer) {
      httpServer.stop(exitKind)
    }
    super.stop(exitKind)
  }
}
