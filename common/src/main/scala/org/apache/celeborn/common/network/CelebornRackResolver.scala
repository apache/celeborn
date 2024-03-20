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

package org.apache.celeborn.common.network

import java.io.File
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.base.Strings
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.net.{CachedDNSToSwitchMapping, DNSToSwitchMapping, NetworkTopology, Node, NodeBase, ScriptBasedMapping, TableMapping}
import org.apache.hadoop.util.ReflectionUtils

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.util.{CelebornHadoopUtils, ThreadUtils}

class CelebornRackResolver(celebornConf: CelebornConf) extends Logging {

  private var rackResolveRefreshThreadPool: ScheduledExecutorService = _

  private var rackResolveLastModifiedTime = 0L

  private val dnsToSwitchMapping: DNSToSwitchMapping = {
    val conf: Configuration = CelebornHadoopUtils.newConfiguration(celebornConf)
    val dnsToSwitchMappingClass =
      conf.getClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        classOf[ScriptBasedMapping],
        classOf[DNSToSwitchMapping])
    val switchMapping = ReflectionUtils.newInstance(dnsToSwitchMappingClass, conf)
      .asInstanceOf[DNSToSwitchMapping]
    val mapping = switchMapping match {
      case c: CachedDNSToSwitchMapping => c
      case o => new CachedDNSToSwitchMapping(o)
    }
    val refreshInterval = celebornConf.rackResolverRefreshInterval
    rackResolveRefreshThreadPool =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-rack-resolver-refresher")
    val fileName = switchMapping match {
      case _: ScriptBasedMapping =>
        conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY)
      case _: TableMapping =>
        conf.get(CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY)
      case _ => null
    }
    if (fileName != null) {
      var scriptFile = new File(fileName)
      rackResolveLastModifiedTime = scriptFile.lastModified()
      rackResolveRefreshThreadPool.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit = {
            scriptFile = new File(fileName)
            if (scriptFile.canRead) {
              val currentLastModifiedTime = scriptFile.lastModified()
              if (currentLastModifiedTime != rackResolveLastModifiedTime) {
                rackResolveLastModifiedTime = currentLastModifiedTime
                mapping.reloadCachedMappings()
              }
            } else {
              logWarning(s"Script file $fileName is not readable, reload cache directly")
              mapping.reloadCachedMappings()
            }
          }
        },
        refreshInterval,
        refreshInterval,
        TimeUnit.MILLISECONDS)
    } else {
      rackResolveRefreshThreadPool.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit = mapping.reloadCachedMappings()
        },
        refreshInterval,
        refreshInterval,
        TimeUnit.MILLISECONDS)
    }
    mapping
  }

  def stop(): Unit = {
    if (null != rackResolveRefreshThreadPool) {
      rackResolveRefreshThreadPool.shutdownNow()
    }
  }

  def resolve(hostName: String): Node = {
    coreResolve(Seq(hostName)).head
  }

  def resolve(hostNames: Seq[String]): Seq[Node] = {
    coreResolve(hostNames)
  }

  def resolveToMap(hostNames: java.util.List[String]): Map[String, Node] = {
    resolveToMap(hostNames.asScala.toSeq)
  }

  def resolveToMap(hostNames: Seq[String]): Map[String, Node] = {
    hostNames.zip(resolve(hostNames)).toMap
  }

  private def coreResolve(hostNames: Seq[String]): Seq[Node] = {
    if (hostNames.isEmpty) {
      return Seq.empty
    }
    val nodes = new ArrayBuffer[Node]
    // dnsToSwitchMapping is thread-safe
    val rNameList = dnsToSwitchMapping.resolve(hostNames.toList.asJava).asScala
    if (rNameList == null || rNameList.isEmpty) {
      hostNames.foreach(nodes += new NodeBase(_, NetworkTopology.DEFAULT_RACK))
      logInfo(s"Got an error when resolving hostNames. " +
        s"Falling back to ${NetworkTopology.DEFAULT_RACK} for all")
    } else {
      for ((hostName, rName) <- hostNames.zip(rNameList)) {
        if (Strings.isNullOrEmpty(rName)) {
          nodes += new NodeBase(hostName, NetworkTopology.DEFAULT_RACK)
          logDebug(s"Could not resolve $hostName. " +
            s"Falling back to ${NetworkTopology.DEFAULT_RACK}")
        } else {
          nodes += new NodeBase(hostName, rName)
        }
      }
    }
    nodes.toList
  }

  def isOnSameRack(primaryHost: String, replicaHost: String): Boolean = {
    val nodes = resolve(Seq(primaryHost, replicaHost))
    val (primaryNode, replicaNode) = (nodes.head, nodes.last)
    if (primaryNode == null || replicaNode == null) {
      false
    } else {
      primaryNode.getNetworkLocation == replicaNode.getNetworkLocation
    }
  }
}
