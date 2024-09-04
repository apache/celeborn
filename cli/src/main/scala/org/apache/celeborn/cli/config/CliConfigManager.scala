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

package org.apache.celeborn.cli.config

import java.io.{File, FileInputStream, FileOutputStream}
import java.time.LocalDateTime
import java.util.Properties

import org.apache.celeborn.cli.common.CliLogging
import org.apache.celeborn.common.util.Utils

case class CliConfig(cliConfigData: Map[String, String])

object CliConfigManager {
  val cliConfigFilePath: String = {
    val basePath = sys.env.getOrElse("CELEBORN_CONF_DIR", sys.env("HOME"))
    if (basePath == sys.env("HOME")) s"$basePath/.celeborn-cli.conf"
    else s"$basePath/celeborn-cli.conf"
  }
}

class CliConfigManager extends CliLogging {

  private val properties = new Properties()

  def loadConfig(): Option[CliConfig] = {
    val file = new File(CliConfigManager.cliConfigFilePath)
    if (!file.exists()) {
      None
    } else {
      Utils.tryWithResources(new FileInputStream(file)) { inputStream =>
        properties.load(inputStream)
        Some(CliConfig(properties.stringPropertyNames().toArray.map(_.toString).map { key =>
          key -> properties.getProperty(key)
        }.toMap))
      }
    }
  }

  private def saveConfig(cliConfig: CliConfig): Unit = {
    try {
      val file = new File(CliConfigManager.cliConfigFilePath)
      if (!file.exists()) {
        file.getParentFile.mkdirs()
        file.createNewFile()
      }
      properties.clear()
      val outputStream = new FileOutputStream(file)
      Utils.tryWithResources(outputStream) { os =>
        cliConfig.cliConfigData.foreach { case (key, value) =>
          properties.setProperty(key, value)
        }
        properties.store(os, s"Last updated conf at ${LocalDateTime.now()}")
      }
    } catch {
      case e: Exception =>
        logError(s"Error saving config: ${e.getMessage}")
    }
  }

  def add(key: String, value: String): Unit = {
    val config = loadConfig().getOrElse(CliConfig(Map()))
    val updatedConfig = config.copy(cliConfigData = config.cliConfigData + (key -> value))
    saveConfig(updatedConfig)
  }

  def remove(key: String): Unit = {
    val config = loadConfig().getOrElse(CliConfig(Map()))
    val updatedConfig = config.copy(cliConfigData = config.cliConfigData - key)
    saveConfig(updatedConfig)
  }

  def get(key: String): Option[String] = {
    loadConfig().flatMap(config => config.cliConfigData.get(key))
  }
}
