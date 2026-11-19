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

package org.apache.celeborn.service.deploy.master.clustermeta.ha

import java.io.FileInputStream
import java.security.KeyStore
import java.security.cert.Certificate
import javax.net.ssl.X509KeyManager

import scala.collection.JavaConverters._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.network.ssl.{SSLFactory, SslSampleConfigs}
import org.apache.celeborn.common.protocol.TransportModuleConstants
import org.apache.celeborn.common.util.Utils

class MasterClusterInfoSuite extends CelebornFunSuite {

  private def sslEnabledKey(module: String): String = s"celeborn.ssl.$module.enabled"

  private val globalSslKey = "celeborn.ssl.enabled"
  private val ratisKey = sslEnabledKey(TransportModuleConstants.RATIS_MODULE)
  private val rpcServiceKey = sslEnabledKey(TransportModuleConstants.RPC_SERVICE_MODULE)

  private def setAll(conf: CelebornConf, entries: java.util.Map[String, String]): Unit = {
    entries.asScala.foreach { case (k, v) => conf.set(k, v) }
  }

  private def ratisSslEnabled(conf: CelebornConf): Boolean = {
    conf.sslEnabled(MasterClusterInfo.ratisSslModule(conf))
  }

  // Certificate presented by the key manager that Ratis TLS would be configured with.
  private def selectedCertificate(conf: CelebornConf): Certificate = {
    val module = MasterClusterInfo.ratisSslModule(conf)
    val factory = SSLFactory.createSslFactory(Utils.fromCelebornConf(conf, module, 1))
    try {
      assert(factory.hasKeyManagers)
      val keyManager = factory.getKeyManagers.get(0).asInstanceOf[X509KeyManager]
      val alias = keyManager.getServerAliases("RSA", null).head
      keyManager.getCertificateChain(alias).head
    } finally {
      factory.destroy()
    }
  }

  private def keyStoreCertificate(path: String): Certificate = {
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    val in = new FileInputStream(path)
    try {
      keyStore.load(in, "password".toCharArray)
    } finally {
      in.close()
    }
    keyStore.getCertificate(keyStore.aliases().nextElement())
  }

  test("ratis ssl is disabled when neither ratis nor rpc_service module is enabled") {
    assert(!ratisSslEnabled(new CelebornConf()))
  }

  test("ratis ssl stays enabled when only rpc_service is enabled") {
    // Legacy deployments configure only rpc_service; they must keep securing Ratis as before.
    val conf = new CelebornConf()
    conf.set(rpcServiceKey, "true")
    assert(ratisSslEnabled(conf))
    assert(MasterClusterInfo.ratisSslModule(conf) === TransportModuleConstants.RPC_SERVICE_MODULE)
  }

  test("ratis ssl is enabled by the dedicated ratis module alone") {
    val conf = new CelebornConf()
    conf.set(ratisKey, "true")
    assert(ratisSslEnabled(conf))
    assert(MasterClusterInfo.ratisSslModule(conf) === TransportModuleConstants.RATIS_MODULE)
  }

  test("ratis ssl is enabled by the ratis module even when rpc_service is explicitly disabled") {
    val conf = new CelebornConf()
    conf.set(ratisKey, "true")
    conf.set(rpcServiceKey, "false")
    assert(ratisSslEnabled(conf))
  }

  test("explicitly disabled ratis module falls back to rpc_service") {
    val conf = new CelebornConf()
    conf.set(ratisKey, "false")
    conf.set(rpcServiceKey, "true")
    assert(ratisSslEnabled(conf))
    assert(MasterClusterInfo.ratisSslModule(conf) === TransportModuleConstants.RPC_SERVICE_MODULE)
  }

  test("global ssl default does not select the ratis module") {
    val conf = new CelebornConf()
    conf.set(globalSslKey, "true")
    setAll(
      conf,
      SslSampleConfigs.createDefaultConfigMapForModule(TransportModuleConstants.RPC_SERVICE_MODULE))
    assert(MasterClusterInfo.ratisSslModule(conf) === TransportModuleConstants.RPC_SERVICE_MODULE)
    assert(ratisSslEnabled(conf))
    assert(
      selectedCertificate(conf) === keyStoreCertificate(SslSampleConfigs.DEFAULT_KEY_STORE_PATH))
  }

  test("global ssl default does not enable ratis ssl when rpc_service is disabled") {
    val conf = new CelebornConf()
    conf.set(globalSslKey, "true")
    conf.set(rpcServiceKey, "false")
    assert(MasterClusterInfo.ratisSslModule(conf) === TransportModuleConstants.RPC_SERVICE_MODULE)
    assert(!ratisSslEnabled(conf))
  }

  test("explicitly enabled ratis module uses its own certificate") {
    val conf = new CelebornConf()
    setAll(
      conf,
      SslSampleConfigs.createDefaultConfigMapForModule(TransportModuleConstants.RPC_SERVICE_MODULE))
    setAll(
      conf,
      SslSampleConfigs.createAnotherConfigMapForModule(TransportModuleConstants.RATIS_MODULE))
    assert(MasterClusterInfo.ratisSslModule(conf) === TransportModuleConstants.RATIS_MODULE)
    assert(
      selectedCertificate(conf) === keyStoreCertificate(SslSampleConfigs.SECOND_KEY_STORE_PATH))
  }
}
