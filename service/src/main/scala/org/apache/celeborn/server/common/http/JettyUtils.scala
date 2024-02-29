package org.apache.celeborn.server.common.http

import java.net.URL
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}

import org.apache.celeborn.common.exception.CelebornException

private[celeborn] object JettyUtils {

  /**
   * Create a handler for serving files from a static directory
   *
   * @param resourceBase the resource directory contains static resource files
   * @param contextPath the content path to set for the handler
   * @return a static [[ServletContextHandler]]
   */
  def createStaticHandler(
      resourceBase: String,
      contextPath: String): ServletContextHandler = {
    val contextHandler = new ServletContextHandler()
    val holder = new ServletHolder(classOf[DefaultServlet])
    Option(Thread.currentThread().getContextClassLoader.getResource(resourceBase)) match {
      case Some(res) =>
        holder.setInitParameter("resourceBase", res.toString)
      case None =>
        throw new CelebornException("Could not find resource path for Web UI: " + resourceBase)
    }
    contextHandler.setContextPath(contextPath)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  def createServletHandler(contextPath: String, servlet: HttpServlet): ServletContextHandler = {
    val handler = new ServletContextHandler()
    val holder = new ServletHolder(servlet)
    handler.setContextPath(contextPath)
    handler.addServlet(holder, "/")
    handler
  }

  def createRedirectHandler(src: String, dest: String): ServletContextHandler = {
    val redirectedServlet = new HttpServlet {
      private def doReq(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        val newURL = new URL(new URL(req.getRequestURL.toString), dest).toString
        resp.sendRedirect(newURL)
      }
      override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        doReq(req, resp)
      }

      override def doPut(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        doReq(req, resp)
      }

      override def doPost(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        doReq(req, resp)
      }

      override def doDelete(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        doReq(req, resp)
      }

      override protected def doTrace(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
        resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
      }
    }

    createServletHandler(src, redirectedServlet)
  }
}
