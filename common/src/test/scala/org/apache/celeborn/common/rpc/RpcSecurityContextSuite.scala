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

import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.{an, convertToAnyShouldWrapper}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.network.sasl.{SaslCredentials, SecretRegistryImpl}
import org.apache.celeborn.common.network.sasl.registration.RegistrationInfo

class RpcSecurityContextSuite extends CelebornFunSuite {

  test("RpcSecurityContext should be created with either client and server sasl contexts") {
    val clientContext = ClientSaslContext(
      "clientAppId",
      new SaslCredentials("user", "password"),
      addRegistrationBootstrap = true)

    val rpcSecurityContext = new RpcSecurityContextBuilder()
      .withClientSaslContext(clientContext)
      .build()

    rpcSecurityContext.clientSaslContext shouldBe Some(clientContext)
    rpcSecurityContext.serverSaslContext shouldBe None
  }

  test("RpcSecurityContext should be created with only client sasl context") {
    val clientContext = ClientSaslContext(
      "clientAppId",
      new SaslCredentials("user", "password"),
      addRegistrationBootstrap = true)

    val rpcSecurityContext = new RpcSecurityContextBuilder()
      .withClientSaslContext(clientContext)
      .build()

    rpcSecurityContext.clientSaslContext shouldBe Some(clientContext)
    rpcSecurityContext.serverSaslContext shouldBe None
  }

  test("RpcSecurityContext should be created with only server sasl context") {
    val serverContext = ServerSaslContext(new SecretRegistryImpl())

    val rpcSecurityContext = new RpcSecurityContextBuilder()
      .withServerSaslContext(serverContext)
      .build()

    rpcSecurityContext.clientSaslContext shouldBe None
    rpcSecurityContext.serverSaslContext shouldBe Some(serverContext)
  }

  test("ClientSaslContext build with valid parameters") {
    val clientContext = new ClientSaslContextBuilder()
      .withSaslUser("user")
      .withSaslPassword("password")
      .withAppId("clientAppId")
      .withAddRegistrationBootstrap(true)
      .withRegistrationInfo(new RegistrationInfo())
      .build()

    clientContext.appId shouldBe "clientAppId"
    clientContext.saslCredentials.getUserId shouldBe "user"
    clientContext.saslCredentials.getPassword shouldBe "password"
    clientContext.addRegistrationBootstrap shouldBe true
    clientContext.registrationInfo shouldNot be(null)
  }

  test("ClientSaslContext build should throw IllegalArgumentException when sasl user/password is not set") {
    an[IllegalArgumentException] should be thrownBy {
      new ClientSaslContextBuilder()
        .withAppId("clientAppId")
        .withAddRegistrationBootstrap(true)
        .withRegistrationInfo(new RegistrationInfo())
        .build()
    }
  }

  test("ClientSaslContext build should throw IllegalArgumentException when app id is not set") {
    an[IllegalArgumentException] should be thrownBy {
      new ClientSaslContextBuilder()
        .withSaslUser("user")
        .withSaslPassword("password")
        .withAddRegistrationBootstrap(true)
        .withRegistrationInfo(new RegistrationInfo())
        .build()
    }
  }

  test("ClientSaslContext build should throw IllegalArgumentException when addRegistrationBootstrap is true but registration info is not set") {
    an[IllegalArgumentException] should be thrownBy {
      new ClientSaslContextBuilder()
        .withSaslUser("user")
        .withSaslPassword("password")
        .withAppId("clientAppId")
        .withAddRegistrationBootstrap(true)
        .build()
    }
  }

  test("ServerSaslContext build should build ServerSaslContext with valid parameters") {
    val serverContext = new ServerSaslContextBuilder()
      .withSecretRegistry(new SecretRegistryImpl())
      .withAddRegistrationBootstrap(true)
      .build()

    serverContext.secretRegistry shouldNot be(null)
    serverContext.addRegistrationBootstrap shouldBe true
  }

  test(
    "ServerSaslContext build should throw IllegalArgumentException when secret registry is not set") {
    an[IllegalArgumentException] should be thrownBy {
      new ServerSaslContextBuilder()
        .withAddRegistrationBootstrap(true)
        .build()
    }
  }

}
