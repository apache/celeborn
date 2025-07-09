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
package org.apache.celeborn.common.rpc

import org.apache.celeborn.common.network.sasl.{SaslCredentials, SecretRegistry}
import org.apache.celeborn.common.network.sasl.registration.RegistrationInfo

/**
 * Represents the security context, combining both client and server contexts.
 *
 * @param clientSaslContext Optional client sasl context.
 * @param serverSaslContext Optional server sasl context.
 */
private[celeborn] case class RpcSecurityContext(
    clientSaslContext: Option[ClientSaslContext] = None,
    serverSaslContext: Option[ServerSaslContext] = None)

/**
 * Represents the SASL context.
 */
private[celeborn] trait SaslContext {}

/**
 * Represents the client SASL context.
 * @param appId     The application id.
 * @param saslCredentials sasl credentials.
 * @param addRegistrationBootstrap Whether to add registration bootstrap.
 */
private[celeborn] case class ClientSaslContext(
    appId: String,
    saslCredentials: SaslCredentials,
    addRegistrationBootstrap: Boolean = false,
    registrationInfo: RegistrationInfo = null) extends SaslContext

/**
 * Represents the server SASL context.
 * @param secretRegistry  The secret registry.
 * @param addRegistrationBootstrap  Whether to add registration bootstrap.
 */
private[celeborn] case class ServerSaslContext(
    secretRegistry: SecretRegistry,
    addRegistrationBootstrap: Boolean = false) extends SaslContext

/**
 * Builder for [[ClientSaslContext]].
 */
private[celeborn] class ClientSaslContextBuilder {
  private var saslUser: String = _
  private var saslPassword: String = _
  private var appId: String = _
  private var addRegistrationBootstrap: Boolean = false
  private var registrationInfo: RegistrationInfo = _

  def withSaslUser(user: String): ClientSaslContextBuilder = {
    this.saslUser = user
    this
  }

  def withSaslPassword(password: String): ClientSaslContextBuilder = {
    this.saslPassword = password
    this
  }

  def withAppId(appId: String): ClientSaslContextBuilder = {
    this.appId = appId
    this
  }

  def withAddRegistrationBootstrap(addRegistrationBootstrap: Boolean): ClientSaslContextBuilder = {
    this.addRegistrationBootstrap = addRegistrationBootstrap
    this
  }

  def withRegistrationInfo(registrationInfo: RegistrationInfo): ClientSaslContextBuilder = {
    this.registrationInfo = registrationInfo
    this
  }

  def build(): ClientSaslContext = {
    if (saslUser == null || saslPassword == null) {
      throw new IllegalArgumentException("Sasl user/password is not set.")
    }
    if (appId == null) {
      throw new IllegalArgumentException("App id is not set.")
    }
    if (addRegistrationBootstrap && registrationInfo == null) {
      throw new IllegalArgumentException("Registration info is not set.")
    }
    ClientSaslContext(
      appId,
      new SaslCredentials(saslUser, saslPassword),
      addRegistrationBootstrap,
      registrationInfo)
  }
}

/**
 * Builder for [[ServerSaslContext]].
 */
private[celeborn] class ServerSaslContextBuilder {
  private var secretRegistry: SecretRegistry = _
  private var addRegistrationBootstrap: Boolean = false

  def withSecretRegistry(secretRegistry: SecretRegistry): ServerSaslContextBuilder = {
    this.secretRegistry = secretRegistry
    this
  }

  def withAddRegistrationBootstrap(addRegistrationBootstrap: Boolean): ServerSaslContextBuilder = {
    this.addRegistrationBootstrap = addRegistrationBootstrap
    this
  }

  def build(): ServerSaslContext = {
    if (secretRegistry == null) {
      throw new IllegalArgumentException("Secret registry is not set.")
    }
    ServerSaslContext(
      secretRegistry,
      addRegistrationBootstrap)
  }
}

/**
 * Builder for [[RpcSecurityContext]].
 */
private[celeborn] class RpcSecurityContextBuilder {
  private var clientSaslContext: Option[ClientSaslContext] = None
  private var serverSaslContext: Option[ServerSaslContext] = None

  def withClientSaslContext(context: ClientSaslContext): RpcSecurityContextBuilder = {
    this.clientSaslContext = Some(context)
    this
  }

  def withServerSaslContext(context: ServerSaslContext): RpcSecurityContextBuilder = {
    this.serverSaslContext = Some(context)
    this
  }

  def build(): RpcSecurityContext = {
    if (clientSaslContext.nonEmpty && serverSaslContext.nonEmpty) {
      throw new IllegalArgumentException("Both client and server sasl context cannot be set.")
    }
    RpcSecurityContext(clientSaslContext, serverSaslContext)
  }
}
