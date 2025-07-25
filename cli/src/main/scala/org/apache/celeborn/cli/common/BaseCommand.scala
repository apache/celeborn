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

import java.util

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import picocli.CommandLine.{Command, ParameterException}
import picocli.CommandLine.Model.CommandSpec

import org.apache.celeborn.rest.v1.model.{DeleteDynamicConfigRequest, HandleResponse, UpsertDynamicConfigRequest}

@Command(mixinStandardHelpOptions = true, versionProvider = classOf[CliVersionProvider])
abstract class BaseCommand extends Runnable with CliLogging {

  private[cli] def upsertDynamicConf(
      commonOptions: CommonOptions,
      spec: CommandSpec,
      upsert: (UpsertDynamicConfigRequest, util.Map[String, String]) => HandleResponse)
      : HandleResponse = {
    if (StringUtils.isBlank(commonOptions.configLevel)) {
      throw new ParameterException(
        spec.commandLine(),
        "Config level must be provided for this command.")
    }
    if (StringUtils.isBlank(commonOptions.upsertConfigs)) {
      throw new ParameterException(
        spec.commandLine(),
        "Configs to upsert must be provided for this command.")
    }
    val upsertConfigs =
      commonOptions.upsertConfigs.split(',').map(_.trim).filter(_.nonEmpty).map { config =>
        val Array(k, v) = config.split(':').map(_.trim)
        k -> v
      }.toMap.asJava
    upsert(
      new UpsertDynamicConfigRequest()
        .level(UpsertDynamicConfigRequest.LevelEnum.fromValue(commonOptions.configLevel))
        .configs(upsertConfigs)
        .tenant(commonOptions.configTenant)
        .name(commonOptions.configName),
      commonOptions.getAuthHeader)
  }

  private[cli] def deleteDynamicConf(
      commonOptions: CommonOptions,
      spec: CommandSpec,
      delete: (DeleteDynamicConfigRequest, util.Map[String, String]) => HandleResponse)
      : HandleResponse = {
    if (StringUtils.isBlank(commonOptions.configLevel)) {
      throw new ParameterException(
        spec.commandLine(),
        "Config level must be provided for this command.")
    }
    if (StringUtils.isBlank(commonOptions.deleteConfigs)) {
      throw new ParameterException(
        spec.commandLine(),
        "Configs to delete must be provided for this command.")
    }
    delete(
      new DeleteDynamicConfigRequest()
        .level(DeleteDynamicConfigRequest.LevelEnum.fromValue(commonOptions.configLevel))
        .configs(util.Arrays.asList[String](commonOptions.deleteConfigs.split(","): _*))
        .tenant(commonOptions.configTenant)
        .name(commonOptions.configName),
      commonOptions.getAuthHeader)
  }
}
