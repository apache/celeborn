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
import org.apache.celeborn.common.network.sasl.SecretRegistry

/**
 * Represents the app registry context, combining both client and server contexts.
 *
 * @param clientAppRegistryContext Optional client app registry context.
 * @param serverAppRegistryRpcContext Optional server app registry rpc context.
 */
private[celeborn] case class RpcAppRegistryContext(
    clientAppRegistryContext: Option[ClientAppRegistryContext] = None,
    serverAppRegistryRpcContext: Option[ServerAppRegistryRpcContext] = None)

/** Represents the app registry context. */
private[celeborn] trait AppRegistryContext {}

/**
 * Represents the client app registry context.
 * @param appId The application id.
 * @param registrationInfo The registration info.
 */
private[celeborn] case class ClientAppRegistryContext(
    appId: String,
    registrationInfo: RegistrationInfo = null) extends AppRegistryContext

/**
 * Represents the server app registry context.
 * @param secretRegistry The app secret registry.
 */
private[celeborn] case class ServerAppRegistryRpcContext(secretRegistry: SecretRegistry)
  extends AppRegistryContext

/**
 * Builder for [[ClientAppRegistryContext]].
 */
private[celeborn] class ClientAppRegistryContextBuilder {
  private var appId: String = _
  private var registrationInfo: RegistrationInfo = _

  def withAppId(appId: String): ClientAppRegistryContextBuilder = {
    this.appId = appId
    this
  }

  def withRegistrationInfo(registrationInfo: RegistrationInfo): ClientAppRegistryContextBuilder = {
    this.registrationInfo = registrationInfo
    this
  }

  def build(): ClientAppRegistryContext = {
    if (appId == null) {
      throw new IllegalArgumentException("App id is not set.")
    }
    if (registrationInfo == null) {
      throw new IllegalArgumentException("Registration info is not set.")
    }
    ClientAppRegistryContext(appId, registrationInfo)
  }
}

/**
 * Builder for [[ServerAppRegistryRpcContext]].
 */
private[celeborn] class ServerAppRegistryRpcContextBuilder {
  private var secretRegistry: SecretRegistry = _

  def withSecretRegistry(secretRegistry: SecretRegistry): ServerAppRegistryRpcContextBuilder = {
    this.secretRegistry = secretRegistry
    this
  }

  def build(): ServerAppRegistryRpcContext = {
    if (secretRegistry == null) {
      throw new IllegalArgumentException("Secret registry is not set.")
    }
    ServerAppRegistryRpcContext(secretRegistry)
  }
}

/**
 * Builder for [[RpcAppRegistryContext]].
 */
private[celeborn] class RpcAppRegistryContextBuilder {
  private var clientAppRegistryContext: Option[ClientAppRegistryContext] = None
  private var serverAppRegistryRpcContext: Option[ServerAppRegistryRpcContext] = None

  def withClientAppRegistryContext(clientAppRegistryContext: ClientAppRegistryContext)
      : RpcAppRegistryContextBuilder = {
    this.clientAppRegistryContext = Some(clientAppRegistryContext)
    this
  }

  def withServerAppRegistryRpcContext(serverAppRegistryRpcContext: ServerAppRegistryRpcContext)
      : RpcAppRegistryContextBuilder = {
    this.serverAppRegistryRpcContext = Some(serverAppRegistryRpcContext)
    this
  }

  def build(): RpcAppRegistryContext = {
    if (clientAppRegistryContext.nonEmpty && serverAppRegistryRpcContext.nonEmpty) {
      throw new IllegalArgumentException(
        "Both client and server app registry context can not be set.")
    }
    RpcAppRegistryContext(clientAppRegistryContext, serverAppRegistryRpcContext)
  }
}
