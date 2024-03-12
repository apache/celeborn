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

package org.apache.celeborn.common.metrics.sink

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import org.apache.celeborn.common.internal.Logging

object ServletUtils extends Logging {
  // Base type for a function that returns something based on an HTTP request. Allows for
  // implicit conversion from many types of functions to jetty Handlers.
  type Responder[T] = HttpServletRequest => T

  class ServletParams[T <: AnyRef](
      val responder: Responder[T],
      val contentType: String,
      val extractFn: T => String = (in: Any) => in.toString) {}

  /** Create a context handler that responds to a request with the given path prefix */
  def createServletHandler[T <: AnyRef](
      path: String,
      servletParams: ServletParams[T]): ServletContextHandler = {
    createServletHandler(path, createServlet(servletParams))
  }

  /** Create a context handler that responds to a request with the given path prefix */
  def createServletHandler(path: String, servlet: HttpServlet): ServletContextHandler = {
    val prefixedPath = if (path == "/") path else path.stripSuffix("/")
    val contextHandler = new ServletContextHandler
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath(prefixedPath)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  private def createServlet[T <: AnyRef](servletParams: ServletParams[T]): HttpServlet = {
    new HttpServlet {
      override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
        try {
          response.setContentType("%s;charset=utf-8".format(servletParams.contentType))
          response.setStatus(HttpServletResponse.SC_OK)
          val result = servletParams.responder(request)
          response.getWriter.print(servletParams.extractFn(result))
        } catch {
          case e: IllegalArgumentException =>
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage)
          case e: Exception =>
            logWarning(s"GET ${request.getRequestURI} failed: $e", e)
            throw e
        }
      }

      override protected def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
        res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
      }
    }
  }

}
