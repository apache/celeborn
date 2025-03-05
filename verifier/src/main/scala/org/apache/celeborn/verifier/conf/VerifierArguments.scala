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

package org.apache.celeborn.verifier.conf

import java.io.File

import scala.collection.Map

import org.apache.celeborn.common.util.Utils

abstract class VerifierArguments(val args: Array[String], val conf: VerifierConf) {

  var propertiesFile: Option[String] = None

  parseArguments(args.toList)
  loadDefaultVerifierProperties(conf, propertiesFile)
  Utils.loadDefaultCelebornProperties(conf.celebornConf)

  private def loadDefaultVerifierProperties(conf: VerifierConf, filePath: Option[String]): Unit = {
    val path = filePath.getOrElse(getVerifierPropertiesFile())
    Option(path).foreach { confFile =>
      Utils.getPropertiesFromFile(confFile).filter(_._1.startsWith("verf.")).foreach(kv =>
        conf.set(kv._1, kv._2))
    }
  }

  private def getVerifierPropertiesFile(env: Map[String, String] = sys.env): String = {
    env.get("CELEBORN_CONF_DIR")
      .orElse(env.get("CELEBORN_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}celeborn-verifier.conf") }
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }

  def parseArguments(args: List[String]): Unit

  def printUsageAndExit(status: Int): Unit
}
