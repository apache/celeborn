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

import java.nio.charset.StandardCharsets
import java.util.Base64
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.authentication.{AnonymousAuthenticationProviderImpl, DefaultTokenCredential, TokenAuthenticationProvider}
import org.apache.celeborn.common.authentication.HttpAuthSchemes._
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.http.HttpAuthUtils.{AUTHORIZATION_HEADER, WWW_AUTHENTICATE_HEADER}

class BearerAuthenticationHandler(providerClass: String)
  extends AuthenticationHandler with Logging {
  private var conf: CelebornConf = _
  private val allowAnonymous = classOf[AnonymousAuthenticationProviderImpl].getName == providerClass
  override val authScheme: HttpAuthScheme = BEARER

  override def init(conf: CelebornConf): Unit = {
    this.conf = conf
  }

  override def authenticationSupported: Boolean = {
    Option(providerClass).exists { _ =>
      try {
        Class.forName(providerClass).isAssignableFrom(classOf[TokenAuthenticationProvider])
        true
      } catch {
        case _: Throwable => false
      }
    }
  }

  override def matchAuthScheme(authorization: String): Boolean = {
    if (authorization == null || authorization.isEmpty) {
      allowAnonymous
    } else {
      super.matchAuthScheme(authorization)
    }
  }

  override def getAuthorization(request: HttpServletRequest): String = {
    val authHeader = request.getHeader(AUTHORIZATION_HEADER)
    if (allowAnonymous && (authHeader == null || authHeader.isEmpty)) {
      ""
    } else {
      super.getAuthorization(request)
    }
  }

  override def authenticate(
      request: HttpServletRequest,
      response: HttpServletResponse): String = {
    var principal: String = null
    val inputToken = Option(getAuthorization(request))
      .map(a => Base64.getDecoder.decode(a.getBytes()))
      .getOrElse(Array.empty[Byte])

    if (!allowAnonymous && inputToken.isEmpty) {
      response.setHeader(WWW_AUTHENTICATE_HEADER, authScheme.toString)
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
    } else {
      val credential = DefaultTokenCredential(
        new String(inputToken, StandardCharsets.UTF_8),
        Map(TokenAuthenticationProvider.CLIENT_IP_PROPERTY -> Option(
          AuthenticationFilter.HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.get()).getOrElse(
          AuthenticationFilter.HTTP_CLIENT_IP_ADDRESS.get())))
      principal = HttpAuthenticationFactory
        .getTokenAuthenticationProvider(providerClass, conf)
        .authenticate(credential).getName
      response.setStatus(HttpServletResponse.SC_OK)
    }
    principal
  }

  override def destroy(): Unit = {}
}
