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

package org.apache.celeborn.server.common.http.authentication

import java.io.IOException
import javax.security.sasl.AuthenticationException
import javax.servlet.{Filter, FilterChain, FilterConfig, ServletException, ServletRequest, ServletResponse}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import javax.ws.rs.HttpMethod

import scala.collection.mutable

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.authentication.HttpAuthSchemes
import org.apache.celeborn.common.authentication.HttpAuthSchemes.{HttpAuthScheme, _}
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.Service
import org.apache.celeborn.server.common.http.HttpAuthUtils.AUTHORIZATION_HEADER

class AuthenticationFilter(conf: CelebornConf, serviceName: String) extends Filter with Logging {
  import AuthenticationFilter._

  private[authentication] val authSchemeHandlers =
    new mutable.HashMap[HttpAuthScheme, AuthenticationHandler]()

  private[authentication] def addAuthHandler(authHandler: AuthenticationHandler): Unit = {
    authHandler.init(conf)
    if (authHandler.authenticationSupported) {
      if (authSchemeHandlers.contains(authHandler.authScheme)) {
        logWarning(s"Authentication handler has been defined for scheme ${authHandler.authScheme}")
      } else {
        logInfo(s"Add authentication handler ${authHandler.getClass.getSimpleName}" +
          s" for scheme ${authHandler.authScheme}")
        authSchemeHandlers.put(authHandler.authScheme, authHandler)
      }
    } else {
      logWarning(s"The authentication handler ${authHandler.getClass.getSimpleName}" +
        s" for scheme ${authHandler.authScheme} is not supported")
    }
  }

  private val authSchemes: Seq[HttpAuthScheme] = serviceName match {
    case Service.MASTER =>
      conf.get(CelebornConf.MASTER_HTTP_AUTH_SUPPORTED_SCHEMES).map(HttpAuthSchemes.withName)
    case Service.WORKER =>
      conf.get(CelebornConf.WORKER_HTTP_AUTH_SUPPORTED_SCHEMES).map(HttpAuthSchemes.withName)
  }
  private val proxyClientIpHeader: String = serviceName match {
    case Service.MASTER => conf.get(CelebornConf.MASTER_HTTP_PROXY_CLIENT_IP_HEADER)
    case Service.WORKER => conf.get(CelebornConf.WORKER_HTTP_PROXY_CLIENT_IP_HEADER)
  }

  private val administrators: Set[String] = serviceName match {
    case Service.MASTER =>
      conf.get(CelebornConf.MASTER_HTTP_AUTH_ADMINISTERS).toSet
    case Service.WORKER =>
      conf.get(CelebornConf.WORKER_HTTP_AUTH_ADMINISTERS).toSet
  }

  private def initAuthHandlers(): Unit = {
    if (authSchemes.contains(HttpAuthSchemes.NEGOTIATE)) {
      serviceName match {
        case Service.MASTER =>
          addAuthHandler(new SpnegoAuthenticationHandler(
            conf.get(CelebornConf.MASTER_HTTP_SPNEGO_KEYTAB).getOrElse(""),
            conf.get(CelebornConf.MASTER_HTTP_SPNEGO_PRINCIPAL).getOrElse("")))
        case Service.WORKER =>
          addAuthHandler(new SpnegoAuthenticationHandler(
            conf.get(CelebornConf.WORKER_HTTP_SPNEGO_KEYTAB).getOrElse(""),
            conf.get(CelebornConf.WORKER_HTTP_SPNEGO_PRINCIPAL).getOrElse("")))
      }
    }
    if (authSchemes.contains(HttpAuthSchemes.BASIC)) {
      serviceName match {
        case Service.MASTER =>
          addAuthHandler(new BasicAuthenticationHandler(
            conf.get(CelebornConf.MASTER_HTTP_AUTH_BASIC_PROVIDER)))
        case Service.WORKER =>
          addAuthHandler(new BasicAuthenticationHandler(
            conf.get(CelebornConf.WORKER_HTTP_AUTH_BASIC_PROVIDER)))
      }
    }
    if (authSchemes.contains(HttpAuthSchemes.BEARER)) {
      serviceName match {
        case Service.MASTER =>
          addAuthHandler(new BearerAuthenticationHandler(
            conf.get(CelebornConf.MASTER_HTTP_AUTH_BEARER_PROVIDER)))
        case Service.WORKER =>
          addAuthHandler(new BearerAuthenticationHandler(
            conf.get(CelebornConf.WORKER_HTTP_AUTH_BEARER_PROVIDER)))
      }
    }
  }

  override def init(filterConfig: FilterConfig): Unit = {
    initAuthHandlers()
  }

  private[celeborn] def getMatchedHandler(authorization: String): Option[AuthenticationHandler] = {
    authSchemeHandlers.values.find(_.matchAuthScheme(authorization))
  }

  /**
   * If the request has a valid authentication token it allows the request to continue to the
   * target resource, otherwise it triggers an authentication sequence using the configured
   * [[AuthenticationHandler]].
   *
   * @param request     the request object.
   * @param response    the response object.
   * @param filterChain the filter chain object.
   * @throws IOException      thrown if an IO error occurred.
   * @throws ServletException thrown if a processing error occurred.
   */
  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      filterChain: FilterChain): Unit = {
    val httpRequest = request.asInstanceOf[HttpServletRequest]
    val httpResponse = response.asInstanceOf[HttpServletResponse]

    if (authSchemeHandlers.isEmpty || BYPASS_API_PATHS.contains(httpRequest.getRequestURI)) {
      filterChain.doFilter(request, response)
      return
    }

    val authorization = httpRequest.getHeader(AUTHORIZATION_HEADER)
    val matchedHandler = getMatchedHandler(authorization).orNull
    HTTP_CLIENT_IP_ADDRESS.set(httpRequest.getRemoteAddr)
    HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.set(httpRequest.getHeader(proxyClientIpHeader))

    try {
      if (matchedHandler == null) {
        logDebug(s"No auth scheme matched for url: ${httpRequest.getRequestURL}")
        httpResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
        httpResponse.sendError(
          HttpServletResponse.SC_UNAUTHORIZED,
          s"No auth scheme matched for $authorization")
      } else {
        HTTP_AUTH_TYPE.set(matchedHandler.authScheme.toString)
        HTTP_CLIENT_IDENTIFIER.set(matchedHandler.authenticate(httpRequest, httpResponse))
        doFilter(filterChain, httpRequest, httpResponse)
      }
    } catch {
      case e: AuthenticationException =>
        httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN)
        HTTP_CLIENT_IDENTIFIER.remove()
        HTTP_CLIENT_IP_ADDRESS.remove()
        HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.remove()
        HTTP_AUTH_TYPE.remove()
        httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, e.getMessage)
    } finally {
      AuthenticationAuditLogger.audit(httpRequest, httpResponse)
    }
  }

  private def isMutativeRequest(httpRequest: HttpServletRequest): Boolean = {
    HttpMethod.POST.equalsIgnoreCase(httpRequest.getMethod) ||
    HttpMethod.PUT.equalsIgnoreCase(httpRequest.getMethod) ||
    HttpMethod.DELETE.equalsIgnoreCase(httpRequest.getMethod) ||
    HttpMethod.PATCH.equalsIgnoreCase(httpRequest.getMethod)
  }

  /**
   * Delegates call to the servlet filter chain. Sub-classes may override this
   * method to perform pre and post tasks.
   *
   * @param filterChain the filter chain object.
   * @param request     the request object.
   * @param response    the response object.
   * @throws IOException      thrown if an IO error occurred.
   * @throws ServletException thrown if a processing error occurred.
   */
  @throws[IOException]
  @throws[ServletException]
  protected def doFilter(
      filterChain: FilterChain,
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    if (isMutativeRequest(request) && !administrators.contains(HTTP_CLIENT_IDENTIFIER.get())) {
      response.setStatus(HttpServletResponse.SC_FORBIDDEN)
      response.sendError(
        HttpServletResponse.SC_FORBIDDEN,
        s"${HTTP_CLIENT_IDENTIFIER.get()} does not have admin privilege to perform ${request.getMethod} action")
    } else {
      filterChain.doFilter(request, response)
    }
  }

  override def destroy(): Unit = {
    if (authSchemeHandlers.nonEmpty) {
      authSchemeHandlers.values.foreach(_.destroy())
      authSchemeHandlers.clear()
    }
  }
}

object AuthenticationFilter {
  private val BYPASS_API_PATHS = Set("/openapi.json", "/openapi.yaml")

  final val HTTP_CLIENT_IP_ADDRESS = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }
  final val HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }
  final val HTTP_CLIENT_IDENTIFIER = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }
  final val HTTP_AUTH_TYPE = new ThreadLocal[String]() {
    override protected def initialValue(): String = null
  }
}
