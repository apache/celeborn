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

package org.apache.celeborn.cli.common

import java.nio.file.{Files, Paths}

import scala.io.Source

import picocli.CommandLine.IVersionProvider

import org.apache.celeborn.common.util.Utils

class CliVersionProvider extends IVersionProvider with CliLogging {

  private val versionPattern = """Celeborn\s+\S+""".r
  private val prefix = "Celeborn CLI"

  override def getVersion: Array[String] = {
    val versionFile = Paths.get(sys.env.getOrElse("CELEBORN_HOME", "") + "/RELEASE")

    if (Files.exists(versionFile)) {
      Utils.tryWithResources(Source.fromFile(versionFile.toFile)) { source =>
        source.getLines().find(line => versionPattern.findFirstIn(line).isDefined) match {
          case Some(matchingLine) =>
            Array(s"$prefix - ${versionPattern.findFirstIn(matchingLine).get}")
          case _ =>
            logInfo("Could not resolve version of Celeborn since RELEASE file did not contain version info.")
            Array(prefix)
        }
      }
    } else {
      logInfo(
        "Could not resolve version of Celeborn since no RELEASE file was found in $CELEBORN_HOME.")
      Array(prefix)
    }
  }
}
