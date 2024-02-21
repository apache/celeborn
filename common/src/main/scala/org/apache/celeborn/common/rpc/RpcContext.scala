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

import org.apache.celeborn.common.network.registration.RegistrationInfo
import org.apache.celeborn.common.network.sasl.{SaslCredentials, SecretRegistry}

/**
 * Represents the rpc context, combining both client and server contexts.
 * @param clientAnonymousContext Optional client anonymous context.
 * @param serverAnonymousRpcContext Optional server anonymous rpc context.
 * @param clientSaslContext Optional client sasl context.
 * @param serverSaslContext Optional server sasl context.
 */
private[celeborn] case class RpcContext(
    clientAnonymousContext: Option[ClientAnonymousContext] = None,
    serverAnonymousRpcContext: Option[ServerAnonymousRpcContext] = None,
    clientSaslContext: Option[ClientSaslContext] = None,
    serverSaslContext: Option[ServerSaslContext] = None)

/**
 * Represents the ANONYMOUS context.
 */
private[celeborn] trait AnonymousContext {}

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

private[celeborn] case class ClientAnonymousContext(
    appId: String,
    registrationInfo: RegistrationInfo = null) extends AnonymousContext

private[celeborn] case class ServerAnonymousRpcContext(secretRegistry: SecretRegistry)
  extends AnonymousContext

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
 * Builder for [[ClientAnonymousContext]].
 */
private[celeborn] class ClientAnonymousContextBuilder {
  private var appId: String = _
  private var registrationInfo: RegistrationInfo = _

  def withAppId(appId: String): ClientAnonymousContextBuilder = {
    this.appId = appId
    this
  }

  def withRegistrationInfo(registrationInfo: RegistrationInfo): ClientAnonymousContextBuilder = {
    this.registrationInfo = registrationInfo
    this
  }

  def build(): ClientAnonymousContext = {
    if (appId == null) {
      throw new IllegalArgumentException("App id is not set.")
    }
    if (registrationInfo == null) {
      throw new IllegalArgumentException("Registration info is not set.")
    }
    ClientAnonymousContext(appId, registrationInfo)
  }
}

/**
 * Builder for [[ServerAnonymousRpcContext]].
 */
private[celeborn] class ServerAnonymousRpcContextBuilder {
  private var secretRegistry: SecretRegistry = _

  def withSecretRegistry(secretRegistry: SecretRegistry): ServerAnonymousRpcContextBuilder = {
    this.secretRegistry = secretRegistry
    this
  }

  def build(): ServerAnonymousRpcContext = {
    if (secretRegistry == null) {
      throw new IllegalArgumentException("Secret registry is not set.")
    }
    ServerAnonymousRpcContext(secretRegistry)
  }
}

/**
 * Builder for [[RpcContext]].
 */
private[celeborn] class RpcContextBuilder {
  private var clientAnonymousContext: Option[ClientAnonymousContext] = None
  private var serverAnonymousRpcContext: Option[ServerAnonymousRpcContext] = None
  private var clientSaslContext: Option[ClientSaslContext] = None
  private var serverSaslContext: Option[ServerSaslContext] = None

  def withClientSaslContext(context: ClientSaslContext): RpcContextBuilder = {
    this.clientSaslContext = Some(context)
    this
  }

  def withServerSaslContext(context: ServerSaslContext): RpcContextBuilder = {
    this.serverSaslContext = Some(context)
    this
  }

  def withClientAnonymousContext(context: ClientAnonymousContext): RpcContextBuilder = {
    this.clientAnonymousContext = Some(context)
    this
  }

  def withServerAnonymousContext(context: ServerAnonymousRpcContext): RpcContextBuilder = {
    this.serverAnonymousRpcContext = Some(context)
    this
  }

  def build(): RpcContext = {
    if ((clientAnonymousContext.nonEmpty && clientSaslContext.nonEmpty) ||
      (serverAnonymousRpcContext.nonEmpty && serverSaslContext.nonEmpty)) {
      throw new IllegalArgumentException("Both anonymous and sasl context cannot be set.")
    }
    if ((clientAnonymousContext.nonEmpty || clientSaslContext.nonEmpty) &&
      (clientSaslContext.nonEmpty || serverSaslContext.nonEmpty)) {
      throw new IllegalArgumentException("Both client and server context cannot be set.")
    }
    RpcContext(
      clientAnonymousContext,
      serverAnonymousRpcContext,
      clientSaslContext,
      serverSaslContext)
  }
}
