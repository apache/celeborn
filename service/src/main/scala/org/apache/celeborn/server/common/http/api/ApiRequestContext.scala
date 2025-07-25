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

import javax.servlet.ServletContext
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.{Context, MediaType, Response}
import javax.ws.rs.ext.{ExceptionMapper, Provider}

import org.eclipse.jetty.server.handler.ContextHandler

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.HttpService

private[celeborn] trait ApiRequestContext extends Logging {
  @Context
  protected var servletContext: ServletContext = _

  @Context
  protected var httpRequest: HttpServletRequest = _

  final protected def httpService: HttpService = HttpServiceContext.get(servletContext)

  protected def normalizeParam(param: String): String = Option(param).map(_.trim).getOrElse("")
}

@Provider
class RestExceptionMapper extends ExceptionMapper[Exception] with Logging {
  override def toResponse(exception: Exception): Response = {
    logWarning("Error occurs on accessing REST API.", exception)
    exception match {
      case e: WebApplicationException =>
        Response.status(e.getResponse.getStatus)
          .`type`(MediaType.APPLICATION_JSON)
          .entity(Map("message" -> e.getMessage))
          .build()
      case e =>
        Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .`type`(MediaType.APPLICATION_JSON)
          .entity(Map("message" -> e.getMessage))
          .build()
    }
  }
}

private[celeborn] object HttpServiceContext {
  private val attribute = getClass.getCanonicalName

  def set(contextHandler: ContextHandler, rs: HttpService): Unit = {
    contextHandler.setAttribute(attribute, rs)
  }

  def get(context: ServletContext): HttpService = {
    context.getAttribute(attribute).asInstanceOf[HttpService]
  }
}
