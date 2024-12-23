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

import javax.ws.rs.{Consumes, DefaultValue, GET, POST, Produces, QueryParam}
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator

import org.apache.celeborn.rest.v1.model.{HandleResponse, LoggerInfo, LoggerInfos}
import org.apache.celeborn.server.common.http.api.ApiRequestContext

@Tag(name = "Logger")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class LoggerResource extends ApiRequestContext {

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[LoggerInfo]))),
    description = "Get the logger level, return all loggers if no name specified.")
  @GET
  def getLoggerLevel(
      @QueryParam("name") name: String,
      @QueryParam("all") @DefaultValue("false") @Parameter(description =
        "Return all logger instances if true, otherwise return all configured loggers.") all: Boolean)
      : LoggerInfos = {
    if (null != name) {
      new LoggerInfos().addLoggersItem(
        new LoggerInfo().name(name).level(LogManager.getLogger(name).getLevel.toString))
    } else {
      val loggerContext = LogManager.getContext(false).asInstanceOf[LoggerContext]
      val loggers =
        if (all) {
          loggerContext.getLoggers.asScala.map { logger =>
            new LoggerInfo().name(logger.getName).level(logger.getLevel.toString)
          }.toSeq
        } else {
          loggerContext.getConfiguration.getLoggers.values().asScala.map { loggerConfig =>
            new LoggerInfo().name(loggerConfig.getName).level(loggerConfig.getLevel.toString)
          }.toSeq
        }
      new LoggerInfos().loggers(loggers.sortBy(_.getName).asJava)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[HandleResponse]))),
    description = "Set the logger level.")
  @POST
  def setLoggerLevel(request: LoggerInfo): HandleResponse = {
    val loggerName = request.getName
    val logger = LogManager.getLogger(loggerName)
    val originalLevel = logger.getLevel
    val newLevel = Level.toLevel(request.getLevel)
    Configurator.setLevel(loggerName, newLevel)
    new HandleResponse().success(true).message(
      s"Set logger `$loggerName` level from `$originalLevel` to `$newLevel`.`")
  }
}
