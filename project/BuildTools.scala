package org.apache.celeborn.build

import java.nio.file.Files

import sbt._
import Keys._

object HelloPlugin extends AutoPlugin {
  override def trigger = allRequirements

  object autoImport {
    val helloGreeting = settingKey[String]("greeting")
    val hello = taskKey[Unit]("say hello")
  }

  import autoImport._
  override lazy val globalSettings: Seq[Setting[_]] = Seq(
    helloGreeting := "hi",
  )

  override lazy val projectSettings: Seq[Setting[_]] = Seq(
    hello := {
      val s = streams.value
      val g = helloGreeting.value
      s.log.info(g)
    }
  )
}

/**
 * This is a SBT AutoPlugin for managing distribution jar files.
 *
 * Usage:
 * 1. Configure the `distributionOutputPath` setting to specify the target directory for distribution jars.
 * {{
 *   set ThisBuild / distributionOutputPath := "/path/to/jars"
 * }}
 * 2. Use the `copyJars` task to copy jar files to the distribution directory.
 * {{
 *   celeborn-worker/copyJars
 * }}
 */
object DistributionToolsPlugin extends AutoPlugin {
  override def trigger = allRequirements

  object autoImport {
    val distributionOutputPath = settingKey[String]("Path for outputting distribution jars.")
    val copyJars = taskKey[Unit]("Copy jars to the specified path.")
  }

  import autoImport._
  val buildLocation = file(".").getAbsoluteFile.getParentFile / "jars"

  override lazy val globalSettings: Seq[Setting[_]] = Seq(
    distributionOutputPath := buildLocation.getPath
  )

  override lazy val projectSettings: Seq[Setting[_]] = Seq(
    copyJars := {
      val log = streams.value.log
      val outputPath = distributionOutputPath.value

      log.info(s"Copying jars to target directory: $outputPath")

      Files.createDirectories(buildLocation.toPath)

      // Copy internal dependency jars
      // Utilize the `Compile` scope to exclude the dependency of the project itself
      (Compile / internalDependencyAsJars).value.files.foreach { jarFile =>
        log.info(s"Copying internal dependency jar: ${jarFile.getName}")
        IO.copyFile(jarFile, buildLocation / jarFile.getName)
      }

      // Copy managed classpath jars
      (Runtime / managedClasspath).value.files.foreach { jarFile =>
        log.info(s"Copying dependency jar: ${jarFile.getName}")
        IO.copyFile(jarFile, buildLocation / jarFile.getName)
      }
    }
  )
}

