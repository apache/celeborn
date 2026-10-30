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

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.celeborn.common.authentication.HttpAuthSchemes
import org.apache.celeborn.server.common.http.authentication.AuthenticationFilter
import org.apache.celeborn.spi.authentication.Credential

object HttpAuthUtils {
  // HTTP header used by the server endpoint during an authentication sequence.
  val WWW_AUTHENTICATE_HEADER = "WWW-Authenticate"
  // HTTP header used by the client endpoint during an authentication sequence.
  val AUTHORIZATION_HEADER = "Authorization"
  // Realm advertised in the Basic authentication challenge (RFC 7617 requires a realm).
  val BASIC_AUTH_REALM = "Celeborn"

  /**
   * Build the value of a single WWW-Authenticate challenge header for an auth scheme.
   *
   * The scheme token is case-insensitive on the wire, so we emit the enumeration name (e.g.
   * NEGOTIATE / BEARER) for most schemes, but RFC 7617 mandates a realm for Basic, so BASIC is
   * emitted with one: `Basic realm="Celeborn"`. This is the challenge a client (curl, HTTP
   * auth handlers) uses to decide it can start an auth handshake.
   */
  def challengeHeaderValue(scheme: HttpAuthSchemes.HttpAuthScheme): String = scheme match {
    case HttpAuthSchemes.BASIC => s"""Basic realm="$BASIC_AUTH_REALM""""
    case _ => scheme.toString
  }

  def getCredentialExtraInfo: JMap[String, String] = {
    Map(Credential.CLIENT_IP_KEY ->
      Option(
        AuthenticationFilter.HTTP_PROXY_HEADER_CLIENT_IP_ADDRESS.get()).getOrElse(
        AuthenticationFilter.HTTP_CLIENT_IP_ADDRESS.get())).asJava
  }
}
