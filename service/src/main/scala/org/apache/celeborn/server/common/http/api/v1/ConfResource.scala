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

package org.apache.celeborn.server.common.http.api.v1

import javax.ws.rs.{Consumes, GET, Path, Produces, QueryParam, ServiceUnavailableException}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.commons.lang3.StringUtils

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.server.common.http.api.ApiRequestContext
import org.apache.celeborn.server.common.http.v1.model.{ConfigData, ConfResponse, DynamicConfig, DynamicConfigResponse}
import org.apache.celeborn.server.common.http.v1.model.DynamicConfig.LevelEnum
import org.apache.celeborn.server.common.service.config.ConfigLevel

@Tag(name = "Conf")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
private[api] class ConfResource extends ApiRequestContext {
  private def configService = httpService.configService

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[ConfResponse]))),
    description = "List the conf setting")
  @GET
  def conf: ConfResponse = {
    new ConfResponse()
      .configs(
        Utils.redact(httpService.conf, httpService.conf.getAll).sortBy(_._1).map { case (n, v) =>
          new ConfigData().name(n).value(v)
        }.asJava)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[DynamicConfigResponse]))),
    description = "List the dynamic configs. " +
      "The parameter level specifies the config level of dynamic configs. " +
      "The parameter tenant specifies the tenant id of TENANT or TENANT_USER level. " +
      "The parameter name specifies the user name of TENANT_USER level. " +
      "Meanwhile, either none or all of the parameter tenant and name are specified for TENANT_USER level.")
  @Path("/dynamic")
  @GET
  def dynamicConf(
      @QueryParam("level") level: String,
      @QueryParam("tenant") tenant: String,
      @QueryParam("name") name: String): DynamicConfigResponse = {
    if (configService == null) {
      throw new ServiceUnavailableException(
        s"Dynamic configuration is disabled. Please check whether to config" +
          s" `${CelebornConf.DYNAMIC_CONFIG_STORE_BACKEND.key}`.")
    } else {
      if (StringUtils.isEmpty(level)) {
        new DynamicConfigResponse()
          .configs(ConfigLevel.values().flatMap { configLevel =>
            getDynamicConfig(configLevel.name(), tenant, name)
          }.toSeq.asJava)
      } else {
        new DynamicConfigResponse()
          .configs(getDynamicConfig(level, tenant, name).asJava)
      }
    }
  }

  private def getDynamicConfig(level: String, tenant: String, name: String): Seq[DynamicConfig] = {
    if (ConfigLevel.SYSTEM.name().equalsIgnoreCase(level)) {
      val config = configService.getSystemConfigFromCache.getConfigs.asScala
      Seq(new DynamicConfig()
        .level(LevelEnum.SYSTEM)
        .configs(config.toSeq.sortBy(_._1).map { case (n, v) =>
          new ConfigData().name(n).value(v)
        }.asJava))
    } else if (ConfigLevel.TENANT.name().equalsIgnoreCase(level)) {
      val tenantConfigs =
        if (StringUtils.isEmpty(tenant)) {
          configService.listRawTenantConfigsFromCache().asScala
        } else {
          List(configService.getRawTenantConfigFromCache(tenant))
        }
      tenantConfigs.sortBy(_.getTenantId).map { tenantConfig =>
        new DynamicConfig()
          .level(LevelEnum.TENANT)
          .desc(s"Tenant: ${tenantConfig.getTenantId}")
          .configs(tenantConfig.getConfigs.asScala.toSeq.sortBy(_._1).map { case (n, v) =>
            new ConfigData().name(n).value(v)
          }.asJava)
      }.toSeq
    } else if (ConfigLevel.TENANT_USER.name().equalsIgnoreCase(level)) {
      val tenantUserConfigs =
        if (StringUtils.isEmpty(tenant) && StringUtils.isEmpty(name)) {
          configService.listRawTenantUserConfigsFromCache().asScala
        } else if (tenant.nonEmpty && name.nonEmpty) {
          List(configService.getRawTenantUserConfigFromCache(tenant, name))
        } else {
          List()
        }
      tenantUserConfigs.sortBy(_.getTenantId).map { tenantUserConfig =>
        new DynamicConfig()
          .level(LevelEnum.TENANT_USER)
          .desc(s"Tenant: ${tenantUserConfig.getTenantId}, User: ${tenantUserConfig.getName}")
          .configs(tenantUserConfig.getConfigs.asScala.toSeq.sortBy(_._1).map { case (n, v) =>
            new ConfigData().name(n).value(v)
          }.asJava)
      }.toSeq
    } else {
      Seq.empty[DynamicConfig]
    }
  }
}
