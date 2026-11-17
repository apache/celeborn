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
import javax.servlet.{FilterChain, ServletRequest, ServletResponse}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.mockito.Mockito.{mock, never, verify, verifyNoInteractions, when}
import org.scalatest.BeforeAndAfterEach

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.authentication.AnonymousAuthenticationProviderImpl
import org.apache.celeborn.common.authentication.HttpAuthSchemes.NEGOTIATE
import org.apache.celeborn.server.common.Service
import org.apache.celeborn.server.common.http.HttpAuthUtils.{AUTHORIZATION_HEADER, WWW_AUTHENTICATE_HEADER}
import org.apache.celeborn.server.common.http.authentication.AuthenticationFilter._

class AuthenticationFilterSuite extends CelebornFunSuite with BeforeAndAfterEach {
  override protected def afterEach(): Unit = {
    try {
      HTTP_CLIENT_IDENTIFIER.remove()
      HTTP_AUTH_TYPE.remove()
      HTTP_CLIENT_IP_ADDRESS.remove()
      HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.remove()
    } finally {
      super.afterEach()
    }
  }

  private def basicFilter(service: String, anonymous: Boolean = false): AuthenticationFilter = {
    val provider =
      if (anonymous) classOf[AnonymousAuthenticationProviderImpl].getName
      else classOf[UserDefinedPasswordAuthenticationProviderImpl].getName
    val conf = new CelebornConf()
      .set(CelebornConf.MASTER_HTTP_AUTH_SUPPORTED_SCHEMES, Seq("BASIC"))
      .set(CelebornConf.WORKER_HTTP_AUTH_SUPPORTED_SCHEMES, Seq("BASIC"))
      .set(CelebornConf.MASTER_HTTP_AUTH_BASIC_PROVIDER, provider)
      .set(CelebornConf.WORKER_HTTP_AUTH_BASIC_PROVIDER, provider)
    val filter = new AuthenticationFilter(conf, service)
    filter.init(null)
    filter
  }

  private def request(
      method: String,
      authorization: String,
      path: String = "/conf"): HttpServletRequest = {
    val request = mock(classOf[HttpServletRequest])
    when(request.getMethod).thenReturn(method)
    when(request.getRequestURI).thenReturn(path)
    when(request.getHeader(AUTHORIZATION_HEADER)).thenReturn(authorization)
    request
  }

  private def basic(credentials: String): String =
    "Basic " + Base64.getEncoder.encodeToString(credentials.getBytes(StandardCharsets.UTF_8))

  private def checkDelegated(
      filter: AuthenticationFilter,
      request: HttpServletRequest,
      expectedUser: String): Unit = {
    var delegated = false
    val chain = new FilterChain {
      override def doFilter(request: ServletRequest, response: ServletResponse): Unit = {
        delegated = true
        assert(HTTP_CLIENT_IDENTIFIER.get() == expectedUser)
      }
    }
    filter.doFilter(request, mock(classOf[HttpServletResponse]), chain)
    assert(delegated)
  }

  for (service <- Seq(Service.MASTER, Service.WORKER)) {
    test(s"$service incomplete Basic credentials do not delegate read requests") {
      val filter = basicFilter(service)
      try {
        for (method <- Seq("GET", "HEAD", "OPTIONS");
          credentials <- Seq("user", "user:", ":password", " : ")) {
          val response = mock(classOf[HttpServletResponse])
          val chain = mock(classOf[FilterChain])
          filter.doFilter(request(method, basic(credentials)), response, chain)
          verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED)
          verify(response).setHeader(WWW_AUTHENTICATE_HEADER, "Basic realm=\"Celeborn\"")
          verifyNoInteractions(chain)
        }
      } finally filter.destroy()
    }

    test(s"$service completed Basic authentication delegates with the authenticated user") {
      val filter = basicFilter(service)
      try {
        checkDelegated(filter, request("GET", basic("user:password")), "user")
      } finally filter.destroy()
    }

    test(s"$service incomplete Negotiate authentication preserves the handler response") {
      val filter = new AuthenticationFilter(new CelebornConf(), service)
      filter.addAuthHandler(new AuthenticationHandler {
        override val authScheme = NEGOTIATE
        override def authenticationSupported: Boolean = true
        override def init(conf: CelebornConf): Unit = {}
        override def destroy(): Unit = {}
        override def authenticate(
            request: HttpServletRequest,
            response: HttpServletResponse): String = {
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED)
          response.setHeader(WWW_AUTHENTICATE_HEADER, "Negotiate server-token")
          null
        }
      })
      try {
        for (method <- Seq("GET", "POST")) {
          val response = mock(classOf[HttpServletResponse])
          val chain = mock(classOf[FilterChain])
          filter.doFilter(request(method, "Negotiate client-token"), response, chain)
          verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED)
          verify(response).setHeader(WWW_AUTHENTICATE_HEADER, "Negotiate server-token")
          verify(response, never()).setStatus(HttpServletResponse.SC_FORBIDDEN)
          verifyNoInteractions(chain)
        }
      } finally filter.destroy()
    }

    test(s"$service explicitly configured anonymous Basic authentication still delegates") {
      val filter = basicFilter(service, anonymous = true)
      try {
        checkDelegated(filter, request("GET", null), "anonymous")
      } finally filter.destroy()
    }

    test(s"$service health checks still bypass authentication") {
      val filter = basicFilter(service)
      val chain = mock(classOf[FilterChain])
      val req = request("GET", null, "/healthz")
      val response = mock(classOf[HttpServletResponse])
      try {
        filter.doFilter(req, response, chain)
        verify(chain).doFilter(req, response)
      } finally filter.destroy()
    }

    test(s"$service requests without configured authentication still delegate") {
      val filter = new AuthenticationFilter(new CelebornConf(), service)
      val chain = mock(classOf[FilterChain])
      val req = request("GET", null)
      val response = mock(classOf[HttpServletResponse])
      try {
        filter.init(null)
        filter.doFilter(req, response, chain)
        verify(chain).doFilter(req, response)
      } finally filter.destroy()
    }
  }
}
