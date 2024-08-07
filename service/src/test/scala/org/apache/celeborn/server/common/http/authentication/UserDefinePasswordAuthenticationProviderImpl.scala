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

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.http.authentication.UserDefinePasswordAuthenticationProviderImpl.VALID_PASSWORD
import org.apache.celeborn.spi.authentication.{BasicPrincipal, Credential, PasswdAuthenticationProvider, PasswordCredential}

class UserDefinePasswordAuthenticationProviderImpl
  extends PasswdAuthenticationProvider with Logging {
  override def authenticate(credential: PasswordCredential): Principal = {
    val clientIp = credential.extraInfo.get(Credential.CLIENT_IP_KEY)
    if (credential.password == VALID_PASSWORD) {
      logInfo(s"Success log in of user: ${credential.username} with clientIp: $clientIp")
      new BasicPrincipal(credential.username)
    } else {
      throw new AuthenticationException("Username or password is not valid!")
    }
  }
}

object UserDefinePasswordAuthenticationProviderImpl {
  val VALID_PASSWORD = "password"
}
