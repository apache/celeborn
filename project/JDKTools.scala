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

package org.apache.celeborn.build

import java.io.FileNotFoundException

import sbt._
import sbt.Keys._

import scala.sys.process._

import scala.util.Try

/**
 * Represents the main plugin to add tooling settings to projects using libraries from the Scala Debugger project.
 */
object JDKTools extends AutoPlugin {
  override def requires = plugins.JvmPlugin

  override def trigger = allRequirements

  override def projectSettings: Seq[Def.Setting[_]] = inConfig(Compile)(settings)

  lazy val settings = baseScalaDebuggerToolsSettings

  lazy val baseScalaDebuggerToolsSettings: Seq[Def.Setting[_]] =
    if (System.getProperty("java.specification.version").startsWith("1."))
      Seq(
        // JDK Dependency (just for sbt, must exist on classpath for execution, cannot be redistributed)
        unmanagedJars += {
          Attributed.blank(JavaTools)
        }
      )
    else
    // on Java 9+, we don't need to do anything at all
      Seq()

  //
  // NOTE: Taken from Ensime Server project (when under BSD 3-clause)
  // https://github.com/ensime/ensime-server/blob/master/project/EnsimeBuild.scala
  //
  // WORKAROUND: https://github.com/typelevel/scala/issues/75
  lazy val JavaTools: File = List(
    // manual
    sys.env.get("JAVA_HOME"),
    sys.env.get("JDK_HOME"),
    // osx
    Try("/usr/libexec/java_home".!!).toOption,
    // fallback
    // sys.props.get("java.home") returns jre home for JDK8
    sys.props.get("java.home").map(new File(_).getParent),
    sys.props.get("java.home")
  ).flatten.map { n =>
    new File(n.trim + "/lib/tools.jar")
  }.find(_.exists()).getOrElse(
    throw new FileNotFoundException(
      """
        |Could not automatically find the JDK/lib/tools.jar.
        |You must explicitly set JDK_HOME or JAVA_HOME.
        """.stripMargin)
  )
}
