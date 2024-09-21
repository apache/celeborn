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

package org.apache.celeborn.server.common.http.api

import java.net.URI

import javax.servlet.ServletConfig
import javax.ws.rs.{GET, Path, PathParam, Produces}
import javax.ws.rs.core.{Application, Context, HttpHeaders, MediaType, Response, UriInfo}
import scala.collection.JavaConverters._
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import io.swagger.v3.jaxrs2.integration.JaxrsOpenApiContextBuilder
import io.swagger.v3.jaxrs2.integration.resources.BaseOpenApiResource
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.integration.api.OpenApiContext
import io.swagger.v3.oas.models.{Components, OpenAPI}
import io.swagger.v3.oas.models.info.{Info, License}
import io.swagger.v3.oas.models.security.{SecurityRequirement, SecurityScheme}
import io.swagger.v3.oas.models.servers.Server
import org.apache.commons.lang3.StringUtils

@Path("/openapi.{type:json|yaml}")
class CelebornOpenApiResource extends BaseOpenApiResource with ApiRequestContext {
  @Context
  protected var config: ServletConfig = _

  @Context
  protected var app: Application = _

  @GET
  @Produces(Array(MediaType.APPLICATION_JSON, "application/yaml"))
  @Operation(hidden = true)
  def getOpenApi(
      @Context headers: HttpHeaders,
      @Context uriInfo: UriInfo,
      @PathParam("type") tpe: String): Response = {

    val ctxId = getContextId(config)
    val ctx: OpenApiContext = new CelebornJaxrsOpenApiContextBuilder()
      .servletConfig(config)
      .application(app)
      .resourcePackages(OpenAPIConfig.packages(httpService.serviceName).toSet.asJava)
      .configLocation(configLocation)
      .openApiConfiguration(openApiConfiguration)
      .ctxId(ctxId)
      .buildContext(true)

    val openApi = setCelebornOpenAPIDefinition(ctx.read(), uriInfo.getBaseUri)

    if (StringUtils.isNotBlank(tpe) && tpe.trim().equalsIgnoreCase("yaml")) {
      Response.status(Response.Status.OK)
        .entity(
          ctx.getOutputYamlMapper()
            .writer(new DefaultPrettyPrinter())
            .writeValueAsString(openApi))
        .`type`("application/yaml")
        .build()
    } else {
      Response.status(Response.Status.OK)
        .entity(
          ctx.getOutputJsonMapper
            .writer(new DefaultPrettyPrinter())
            .writeValueAsString(openApi))
        .`type`(MediaType.APPLICATION_JSON_TYPE)
        .build()
    }
  }

  private def setCelebornOpenAPIDefinition(openApi: OpenAPI, requestBaseUri: URI): OpenAPI = {
    val httpScheme = if (httpService.httpSslEnabled()) "https:" else "http:"
    val requestBaseUrl = s"$httpScheme${requestBaseUri.getSchemeSpecificPart}"
    val apiUrls = List(requestBaseUrl, s"$httpScheme//${httpService.connectionUrl}/").distinct
    openApi.info(
      new Info().title(
        s"Apache Celeborn REST API Documentation")
        .description(s"Role: ${httpService.serviceName}")
        .license(
          new License().name("Apache License 2.0")
            .url("https://www.apache.org/licenses/LICENSE-2.0.txt")))
      .servers(apiUrls.map(url => new Server().url(url)).asJava)
      .components(Option(openApi.getComponents).getOrElse(new Components())
        .addSecuritySchemes(
          "BasicAuth",
          new SecurityScheme()
            .`type`(SecurityScheme.Type.HTTP)
            .scheme("Basic"))
        .addSecuritySchemes(
          "BearerAuth",
          new SecurityScheme()
            .`type`(SecurityScheme.Type.HTTP)
            .scheme("Bearer")
            .bearerFormat("JWT")))
      .addSecurityItem(new SecurityRequirement()
        .addList("BasicAuth")
        .addList("BearerAuth"))
  }
}

class CelebornJaxrsOpenApiContextBuilder
  extends JaxrsOpenApiContextBuilder[CelebornJaxrsOpenApiContextBuilder]
