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

import java.nio.file.Files

import sbt._
import Keys._

/**
 * This is a SBT AutoPlugin for managing distribution jar files.
 *
 * Usage:
 * 1. into sub project.
 * {{
 *   project celeborn-worker
 * }}
 * 2. Configure the `distributionOutputPath` setting to specify the target directory for distribution jars.
 * {{
 *   set distributionOutputPath := Option(new File("/path/to/jars"))
 * }}
 * 3. Use the `copyJars` task to copy jar files to the distribution directory.
 * {{
 *   copyJars
 * }}
 */
object DistributionToolsPlugin extends AutoPlugin {
  override def trigger = allRequirements

  object autoImport {
    val distributionOutputPath = taskKey[Option[File]]("Path for outputting distribution jars.")
    val copyJars = taskKey[Unit]("Copy jars to the specified path.")
  }

  import autoImport._

  override lazy val globalSettings: Seq[Setting[_]] = Seq(
    distributionOutputPath := None
  )

  override lazy val projectSettings: Seq[Setting[_]] = Seq(
    copyJars := {
      val log = streams.value.log
      val outputPath = distributionOutputPath.value.getOrElse {
        crossTarget.value / "jars"
      }

      log.info(s"Copying jars to target directory: $outputPath")

      Files.createDirectories(outputPath.toPath)

      // Copy internal dependency jars
      // Utilize the `Compile` scope to exclude the dependency of the project itself
      (Compile / internalDependencyAsJars).value.files.foreach { jarFile =>
        log.info(s"Copying internal dependency jar: ${jarFile.getName}")
        IO.copyFile(jarFile, outputPath / jarFile.getName)
      }

      // Copy managed classpath jars
      (Runtime / managedClasspath).value.files.foreach { jarFile =>
        log.info(s"Copying dependency jar: ${jarFile.getName}")
        IO.copyFile(jarFile, outputPath / jarFile.getName)
      }
    }
  )
}

