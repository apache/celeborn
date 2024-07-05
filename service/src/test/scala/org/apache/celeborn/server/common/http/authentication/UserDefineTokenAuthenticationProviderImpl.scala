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

import java.security.Principal
import javax.security.sasl.AuthenticationException

import org.apache.celeborn.common.authentication.{BasicPrincipal, Credential, TokenAuthenticationProvider, TokenCredential}
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.http.authentication.UserDefineTokenAuthenticationProviderImpl.VALID_TOKEN

class UserDefineTokenAuthenticationProviderImpl extends TokenAuthenticationProvider with Logging {
  override def authenticate(credential: TokenCredential): Principal = {
    val clientIp =
      credential.extraInfo.getOrElse(Credential.CLIENT_IP_PROPERTY, null)
    if (credential.token == VALID_TOKEN) {
      logInfo(s"Success log in of token: ${credential.token} with clientIp: $clientIp")
      new BasicPrincipal("user")
    } else {
      throw new AuthenticationException("Token is not valid!")
    }
  }
}

object UserDefineTokenAuthenticationProviderImpl {
  val VALID_TOKEN = "token"
}
