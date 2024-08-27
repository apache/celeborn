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

import java.io.{File, PrintWriter}

import scala.io.Source

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.celeborn.cli.config.CliConfigManager.cliConfigFilePath

case class CliConfig(@JsonProperty("cliConfigData") cliConfigData: Map[String, String])

object CliConfigManager {
  val cliConfigFilePath = s"${sys.env.getOrElse("CELEBORN_CONF_DIR", "HOME")}/celeborn-cli.conf"
}

class CliConfigManager {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  def loadConfig(): Option[CliConfig] = {
    val file = new File(cliConfigFilePath)
    if (!file.exists()) {
      None
    } else {
      try {
        val source = Source.fromFile(cliConfigFilePath)
        val jsonString = source.getLines().mkString
        source.close()
        Some(mapper.readValue(jsonString, classOf[CliConfig]))
      } catch {
        case e: Exception =>
          println(s"Error loading config: ${e.getMessage}")
          None
      }
    }
  }

  private def saveConfig(config: CliConfig): Unit = {
    try {
      val file = new File(cliConfigFilePath)
      if (!file.exists()) {
        file.getParentFile.mkdirs()
        file.createNewFile()
      }
      val jsonString = mapper.writeValueAsString(config)
      val writer = new PrintWriter(file)
      writer.write(jsonString)
      writer.close()
    } catch {
      case e: Exception =>
        println(s"Error saving config: ${e.getMessage}")
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
