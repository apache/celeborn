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

import java.io.{File, FileOutputStream}
import java.security.{KeyStore, PrivateKey}
import java.security.cert.X509Certificate
import java.util.Date

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.Utils

trait SslTestHelper extends AnyFunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with Logging {
  val sslEnabled: Boolean = false

  val keyStoreFile = new File(
    Utils.createTempDir(System.getProperty("java.io.tmpdir"), "celeborn_test"),
    "celeborn-test.jks")
  val keyStorePassword = "my_password"

  if (sslEnabled) {
    System.setProperty("jdk.tls.server.protocols", "TLSv1.2")
    System.setProperty("jdk.tls.client.protocols", "TLSv1.2")
    generateKeyStore()
  }

  override protected def afterAll(): Unit = {
    if (sslEnabled) {
      System.clearProperty("jdk.tls.server.protocols")
      System.clearProperty("jdk.tls.client.protocols")
    }
    super.afterAll()
  }

  private def generateKeyStore(): Unit = {
    val certAndKeyGenClass = Class.forName("sun.security.tools.keytool.CertAndKeyGen")
    val x500NameClass = Class.forName("sun.security.x509.X500Name")
    val CONSTRUCTOR = certAndKeyGenClass.getConstructor(classOf[String], classOf[String])
    val GENERATE_METHOD = certAndKeyGenClass.getMethod("generate", Integer.TYPE)
    val GET_PRIVATE_KEY_METHOD = certAndKeyGenClass.getMethod("getPrivateKey")
    val GET_SELF_CERTIFICATE_METHOD = certAndKeyGenClass.getMethod(
      "getSelfCertificate",
      x500NameClass,
      classOf[Date],
      java.lang.Long.TYPE)
    val X500_NAME_CONSTRUCTOR = x500NameClass.getConstructor(
      classOf[String],
      classOf[String],
      classOf[String],
      classOf[String],
      classOf[String],
      classOf[String])

    val ks = KeyStore.getInstance("jks")
    ks.load(null, null)

    val certAndKeyGen = CONSTRUCTOR.newInstance("RSA", "SHA1WithRSA")
    GENERATE_METHOD.invoke(certAndKeyGen, Integer.valueOf(1024))
    val privateKey = GET_PRIVATE_KEY_METHOD.invoke(certAndKeyGen).asInstanceOf[PrivateKey]

    val chain = Array(GET_SELF_CERTIFICATE_METHOD.invoke(
      certAndKeyGen,
      X500_NAME_CONSTRUCTOR.newInstance("", "", "", "", "", ""),
      new Date(),
      java.lang.Long.valueOf(24 * 60 * 60)).asInstanceOf[X509Certificate])

    // store away the key store
    val fos = new FileOutputStream(keyStoreFile)
    ks.setKeyEntry("celeborn-test", privateKey, keyStorePassword.toCharArray, chain.toArray)
    ks.store(fos, keyStorePassword.toCharArray)
    fos.close()
  }
}
