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

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.server.{Handler, Request}
import org.eclipse.jetty.server.handler.HandlerWrapper

import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.reflect.DynConstructors
import org.apache.celeborn.spi.authentication.{PasswdAuthenticationProvider, TokenAuthenticationProvider}

object HttpAuthenticationFactory {
  def wrapHandler(handler: Handler): HandlerWrapper = {
    new HandlerWrapper {
      _handler = handler

      override def handle(
          target: String,
          baseRequest: Request,
          request: HttpServletRequest,
          response: HttpServletResponse): Unit = {

        try {
          handler.handle(target, baseRequest, request, response)
        } finally {
          AuthenticationFilter.HTTP_CLIENT_IDENTIFIER.remove()
          AuthenticationFilter.HTTP_CLIENT_IP_ADDRESS.remove()
          AuthenticationFilter.HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.remove()
          AuthenticationFilter.HTTP_AUTH_TYPE.remove()
        }
      }

      override def doStart(): Unit = {
        super.doStart()
        handler.start()
      }

      override def doStop(): Unit = {
        handler.stop()
        super.doStop()
      }
    }
  }

  def getPasswordAuthenticationProvider(providerClass: String): PasswdAuthenticationProvider = {
    createAuthenticationProvider(providerClass, classOf[PasswdAuthenticationProvider])
  }

  def getTokenAuthenticationProvider(providerClass: String): TokenAuthenticationProvider = {
    createAuthenticationProvider(providerClass, classOf[TokenAuthenticationProvider])
  }

  private def createAuthenticationProvider[T](className: String, expected: Class[T]): T = {
    try {
      DynConstructors.builder(expected)
        .impl(className)
        .buildChecked[T]()
        .newInstance()
    } catch {
      case e: Exception =>
        throw new CelebornException(
          s"$className must extend of ${expected.getName}",
          e)
    }
  }
}
