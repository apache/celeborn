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

import java.nio.file.Files
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale

import scala.util.Properties

import sbtassembly.AssemblyPlugin.autoImport._
import sbtprotoc.ProtocPlugin.autoImport._

import sbt._
import sbt.Keys._
import Utils._
import CelebornCommonSettings._
// import sbt.Keys.streams

object Dependencies {

  val zstdJniVersion = sparkClientProjects.map(_.zstdJniVersion).getOrElse("1.5.2-1")
  val lz4JavaVersion = sparkClientProjects.map(_.lz4JavaVersion).getOrElse("1.8.0")

  // Dependent library versions
  val commonsCompressVersion = "1.4.1"
  val commonsCryptoVersion = "1.0.0"
  val commonsIoVersion = "2.13.0"
  val commonsLoggingVersion = "1.1.3"
  val commonsLang3Version = "3.12.0"
  val findbugsVersion = "1.3.9"
  val guavaVersion = "32.1.3-jre"
  val hadoopVersion = "3.3.6"
  val javaxServletVersion = "3.1.0"
  val junitInterfaceVersion = "0.13.3"
  // don't forget update `junitInterfaceVersion` when we upgrade junit
  val junitVersion = "4.13.2"
  val leveldbJniVersion = "1.8"
  val log4j2Version = "2.17.2"
  val jdkToolsVersion = "0.1"
  val metricsVersion = "3.2.6"
  val mockitoVersion = "4.11.0"
  val nettyVersion = "4.1.101.Final"
  val ratisVersion = "2.5.1"
  val roaringBitmapVersion = "0.9.32"
  val rocksdbJniVersion = "8.5.3"
  val jacksonVersion = "2.15.3"
  val scalatestMockitoVersion = "1.17.14"
  val scalatestVersion = "3.2.16"
  val slf4jVersion = "1.7.36"
  val snakeyamlVersion = "2.2"
  val snappyVersion = "1.1.10.5"

  // Versions for proto
  val protocVersion = "3.21.7"
  val protoVersion = "3.21.7"
  
  val commonsCompress = "org.apache.commons" % "commons-compress" % commonsCompressVersion
  val commonsCrypto = "org.apache.commons" % "commons-crypto" % commonsCryptoVersion excludeAll(
    ExclusionRule("net.java.dev.jna", "jna"))
  val commonsIo = "commons-io" % "commons-io" % commonsIoVersion
  val commonsLang3 = "org.apache.commons" % "commons-lang3" % commonsLang3Version
  val commonsLogging = "commons-logging" % "commons-logging" % commonsLoggingVersion
  val jdkTools = "com.github.olivergondza" % "maven-jdk-tools-wrapper" % jdkToolsVersion
  val findbugsJsr305 = "com.google.code.findbugs" % "jsr305" % findbugsVersion
  val guava = "com.google.guava" % "guava" % guavaVersion excludeAll(
    ExclusionRule("org.checkerframework", "checker-qual"),
    ExclusionRule("org.codehaus.mojo", "animal-sniffer-annotations"),
    ExclusionRule("com.google.errorprone", "error_prone_annotations"),
    ExclusionRule("com.google.guava", "listenablefuture"),
    ExclusionRule("com.google.j2objc", "j2objc-annotations"))
  val hadoopClientApi = "org.apache.hadoop" % "hadoop-client-api" % hadoopVersion
  val hadoopClientRuntime = "org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion
  val hadoopMapreduceClientApp = "org.apache.hadoop" % "hadoop-mapreduce-client-app" % hadoopVersion excludeAll(
    ExclusionRule("io.netty", "netty-transport-native-epoll"),
    ExclusionRule("com.google.guava", "guava"),
    ExclusionRule("com.fasterxml.jackson.core", "jackson-annotations"),
    ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"),
    ExclusionRule("jline", "jline"),
    ExclusionRule("log4j", "log4j"),
    ExclusionRule("org.slf4j", "slf4j-log4j12"))
  val ioDropwizardMetricsCore = "io.dropwizard.metrics" % "metrics-core" % metricsVersion
  val ioDropwizardMetricsGraphite = "io.dropwizard.metrics" % "metrics-graphite" % metricsVersion
  val ioDropwizardMetricsJvm = "io.dropwizard.metrics" % "metrics-jvm" % metricsVersion
  val ioNetty = "io.netty" % "netty-all" % nettyVersion excludeAll(
    ExclusionRule("io.netty", "netty-handler-ssl-ocsp"))
  val javaxServletApi = "javax.servlet" % "javax.servlet-api" % javaxServletVersion
  val leveldbJniAll = "org.fusesource.leveldbjni" % "leveldbjni-all" % leveldbJniVersion
  val log4j12Api = "org.apache.logging.log4j" % "log4j-1.2-api" % log4j2Version
  val log4jSlf4jImpl = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j2Version
  val lz4Java = "org.lz4" % "lz4-java" % lz4JavaVersion
  val protobufJava = "com.google.protobuf" % "protobuf-java" % protoVersion
  val ratisClient = "org.apache.ratis" % "ratis-client" % ratisVersion
  val ratisCommon = "org.apache.ratis" % "ratis-common" % ratisVersion
  val ratisGrpc = "org.apache.ratis" % "ratis-grpc" % ratisVersion
  val ratisNetty = "org.apache.ratis" % "ratis-netty" % ratisVersion
  val ratisServer = "org.apache.ratis" % "ratis-server" % ratisVersion
  val ratisShell = "org.apache.ratis" % "ratis-shell" % ratisVersion excludeAll(
    ExclusionRule("org.slf4j", "slf4j-simple"))
  val roaringBitmap = "org.roaringbitmap" % "RoaringBitmap" % roaringBitmapVersion
  val rocksdbJni = "org.rocksdb" % "rocksdbjni" % rocksdbJniVersion
  val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
  val jacksonCore = "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion
  val jacksonAnnotations = "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion
  val jacksonModule = "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
  val scalaReflect = "org.scala-lang" % "scala-reflect" % projectScalaVersion
  val slf4jApi = "org.slf4j" % "slf4j-api" % slf4jVersion
  val slf4jJulToSlf4j = "org.slf4j" % "jul-to-slf4j" % slf4jVersion
  val slf4jJclOverSlf4j = "org.slf4j" % "jcl-over-slf4j" % slf4jVersion
  val snakeyaml = "org.yaml" % "snakeyaml" % snakeyamlVersion
  val snappyJava = "org.xerial.snappy" % "snappy-java" % snappyVersion
  val zstdJni = "com.github.luben" % "zstd-jni" % zstdJniVersion

  // Test dependencies
  // https://www.scala-sbt.org/1.x/docs/Testing.html
  val junitInterface = "com.github.sbt" % "junit-interface" % junitInterfaceVersion
  val junit = "junit" % "junit" % junitVersion
  val mockitoCore = "org.mockito" % "mockito-core" % mockitoVersion
  val mockitoInline = "org.mockito" % "mockito-inline" % mockitoVersion
  val scalatestMockito = "org.mockito" %% "mockito-scala-scalatest" % scalatestMockitoVersion
  val scalatest = "org.scalatest" %% "scalatest" % scalatestVersion
}

object CelebornCommonSettings {

  // Scala versions
  val SCALA_2_11_12 = "2.11.12"
  val SCALA_2_12_10 = "2.12.10"
  val SCALA_2_12_15 = "2.12.15"
  val SCALA_2_12_17 = "2.12.17"
  val SCALA_2_12_18 = "2.12.18"
  val scala213 = "2.13.5"
  val ALL_SCALA_VERSIONS = Seq(SCALA_2_11_12, SCALA_2_12_10, SCALA_2_12_15, SCALA_2_12_17, SCALA_2_12_18, scala213)

  val DEFAULT_SCALA_VERSION = SCALA_2_12_15

  val projectScalaVersion = defaultScalaVersion()

  scalaVersion := projectScalaVersion

  autoScalaLibrary := false
  
  // crossScalaVersions must be set to Nil on the root project
  crossScalaVersions := Nil

  lazy val commonSettings = Seq(
    organization := "org.apache.celeborn",
    scalaVersion := projectScalaVersion,
    crossScalaVersions := ALL_SCALA_VERSIONS,
    fork := true,
    scalacOptions ++= Seq("-target:jvm-1.8"),
    javacOptions ++= Seq("-encoding", UTF_8.name(), "-source", "1.8", "-g"),
  
    // -target cannot be passed as a parameter to javadoc. See https://github.com/sbt/sbt/issues/355
    Compile / compile / javacOptions ++= Seq("-target", "1.8"),

    dependencyOverrides := Seq(
      Dependencies.commonsCompress,
      Dependencies.commonsLogging,
      Dependencies.findbugsJsr305,
      Dependencies.slf4jApi),

    // Make sure any tests in any project that uses Spark is configured for running well locally
    Test / javaOptions ++= Seq(
      "-Xmx4g",
      "-XX:+IgnoreUnrecognizedVMOptions",
      "--add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true"
    ),
  
    testOptions += Tests.Argument("-oF"),

    Test / testOptions += Tests.Argument("-oDF"),
    Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),

    // Don't execute in parallel since we can't have multiple Sparks in the same JVM
    Test / parallelExecution := false,

    javaOptions += "-Xmx4g",

    // Configurations to speed up tests and reduce memory footprint
    Test / javaOptions ++= Seq(
      "-Xmx4g"
    ),

    Test / javaOptions ++= Seq(
      "-Dspark.shuffle.sort.io.plugin.class="
        + sys.props.getOrElse("spark.shuffle.plugin.class", "org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO"),
    ),

    Test / envVars += ("IS_TESTING", "1")
  )

  ////////////////////////////////////////////////////////
  //                 Release settings                   //
  ////////////////////////////////////////////////////////

  lazy val releaseSettings = Seq(
    publishMavenStyle := true,
    publishArtifact := true,
    Test / publishArtifact := false,
    credentials += {
      val host = publishTo.value.map {
        case repo: MavenRepo => scala.util.Try(new java.net.URL(repo.root)).map(_.getHost).getOrElse("repository.apache.org")
        case _ => "repository.apache.org"
      }.get

      Credentials(
        "Sonatype Nexus Repository Manager",
        host,
        sys.env.getOrElse("ASF_USERNAME", ""),
        sys.env.getOrElse("ASF_PASSWORD", "")),
    },
    publishTo := {
      if (isSnapshot.value) {
        val publishUrl = sys.env.getOrElse("SONATYPE_SNAPSHOTS_URL", "https://repository.apache.org/content/repositories/snapshots")
        Some(("snapshots" at publishUrl).withAllowInsecureProtocol(true))
      } else {
        val publishUrl = sys.env.getOrElse("SONATYPE_RELEASES_URL", "https://repository.apache.org/service/local/staging/deploy/maven2")
        Some(("releases" at publishUrl).withAllowInsecureProtocol(true))
      }
    },
    licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0")),
      pomExtra :=
        <url>https://celeborn.apache.org/</url>
        <scm>
          <url>git@github.com:apache/incubator-celeborn.git</url>
          <connection>scm:git:git@github.com:apache/incubator-celeborn.git</connection>
        </scm>
  )

  lazy val protoSettings = Seq(
    // Setting version for the protobuf compiler
    PB.protocVersion := Dependencies.protocVersion,
    // set proto sources path
    Compile / PB.protoSources := Seq(sourceDirectory.value / "main" / "proto"),
    Compile / PB.targets := Seq(PB.gens.java -> (Compile / sourceManaged).value)
  )

  lazy val commonUnitTestDependencies = Seq(
    Dependencies.mockitoCore % "test",
    Dependencies.scalatest % "test",
    Dependencies.junit % "test",
    // https://www.scala-sbt.org/1.x/docs/Testing.html
    Dependencies.junitInterface % "test")
}

object CelebornBuild extends sbt.internal.BuildDef {
  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    Seq(
      CelebornCommon.common,
      CelebornClient.client,
      CelebornService.service,
      CelebornWorker.worker,
      CelebornMaster.master) ++ maybeSparkClientModules ++ maybeFlinkClientModules ++ maybeMRClientModules
  }
  
  // ThisBuild / parallelExecution := false

  // scalaVersion := "2.11.12"

  // autoScalaLibrary := false

  crossScalaVersions := Nil

  // load user-defined Profiles
  // loadProfiles()
}

object Utils {
  val profiles = {
    val profiles = Properties.envOrNone("SBT_MAVEN_PROFILES")
      .orElse(Properties.propOrNone("sbt.maven.profiles")) match {
        case None => Seq("sbt")
        case Some(v) =>
          v.split("(\\s+|,)").filterNot(_.isEmpty).map(_.trim.replaceAll("-P", "")).toSeq
      }
      if (profiles.contains("jdwp-test-debug")) {
        sys.props.put("test.jdwp.enabled", "true")
      }
      profiles
  }
  val SPARK_VERSION = profiles.filter(_.startsWith("spark")).headOption

  lazy val sparkClientProjects = SPARK_VERSION match {
    case Some("spark-2.4") => Some(Spark24)
    case Some("spark-3.0") => Some(Spark30)
    case Some("spark-3.1") => Some(Spark31)
    case Some("spark-3.2") => Some(Spark32)
    case Some("spark-3.3") => Some(Spark33)
    case Some("spark-3.4") => Some(Spark34)
    case Some("spark-3.5") => Some(Spark35)
    case _ => None
  }

  lazy val maybeSparkClientModules: Seq[Project] = sparkClientProjects.map(_.modules).getOrElse(Seq.empty)

  val FLINK_VERSION = profiles.filter(_.startsWith("flink")).headOption

  lazy val flinkClientProjects = FLINK_VERSION match {
    case Some("flink-1.14") => Some(Flink114)
    case Some("flink-1.15") => Some(Flink115)
    case Some("flink-1.17") => Some(Flink117)
    case Some("flink-1.18") => Some(Flink118)
    case _ => None
  }

  lazy val maybeFlinkClientModules: Seq[Project] = flinkClientProjects.map(_.modules).getOrElse(Seq.empty)

  val MR_VERSION = profiles.filter(_.startsWith("mr")).headOption

  lazy val mrClientProjects = MR_VERSION match {
    case Some("mr") => Some(MRClientProjects)
    case _ => None
  }

  lazy val maybeMRClientModules: Seq[Project] = mrClientProjects.map(_.modules).getOrElse(Seq.empty)

  def defaultScalaVersion(): String = {
    // 1. Inherit the scala version of the spark project
    // 2. if the spark profile not specified, using the DEFAULT_SCALA_VERSION
    val v = sparkClientProjects.map(_.sparkProjectScalaVersion).getOrElse(DEFAULT_SCALA_VERSION)
    require(ALL_SCALA_VERSIONS.contains(v), s"found not allow scala version: $v")
    v
  }
}

object CelebornCommon {
  lazy val common = Project("celeborn-common", file("common"))
    .settings (
      commonSettings,
      protoSettings,
      libraryDependencies ++= Seq(
        Dependencies.protobufJava,
        Dependencies.findbugsJsr305,
        Dependencies.guava,
        Dependencies.commonsIo,
        Dependencies.ioDropwizardMetricsCore,
        Dependencies.ioDropwizardMetricsGraphite,
        Dependencies.ioDropwizardMetricsJvm,
        Dependencies.ioNetty,
        Dependencies.commonsCrypto,
        Dependencies.commonsLang3,
        Dependencies.hadoopClientApi,
        Dependencies.hadoopClientRuntime,
        Dependencies.jdkTools,
        Dependencies.ratisClient,
        Dependencies.ratisCommon,
        Dependencies.leveldbJniAll,
        Dependencies.roaringBitmap,
        Dependencies.scalaReflect,
        Dependencies.slf4jJclOverSlf4j,
        Dependencies.slf4jJulToSlf4j,
        Dependencies.slf4jApi,
        Dependencies.snakeyaml,
        Dependencies.snappyJava,
        Dependencies.jacksonModule,
        Dependencies.jacksonCore,
        Dependencies.jacksonDatabind,
        Dependencies.jacksonAnnotations,
        Dependencies.log4jSlf4jImpl % "test",
        Dependencies.log4j12Api % "test"
      ) ++ commonUnitTestDependencies,

      Compile / sourceGenerators += Def.task {
        val file = (Compile / sourceManaged).value / "org" / "apache" / "celeborn" / "package.scala"
        streams.value.log.info(s"geneate version information file ${file.toPath}")
        IO.write(file,
          s"""package org.apache
             |
             |package object celeborn {
             |  val VERSION = "${version.value}"
             |}
             |""".stripMargin)
        Seq(file)
        // generate version task depends on PB generate to avoid concurrency generate source files
      }.dependsOn(Compile / PB.generate),
  
      // a task to show current profiles
      printProfiles := {
        val message = profiles.mkString("", " ", "")
        println("compile with profiles: %s".format(message))
      }
    )

    lazy val printProfiles = taskKey[Unit]("Prints Profiles")
}

object CelebornClient {
  lazy val client = Project("celeborn-client", file("client"))
    // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
    .dependsOn(CelebornCommon.common % "test->test;compile->compile")
    .settings (
      commonSettings,
      libraryDependencies ++= Seq(
        Dependencies.ioNetty,
        Dependencies.guava,
        Dependencies.lz4Java,
        Dependencies.zstdJni,
        Dependencies.commonsLang3,
        Dependencies.log4jSlf4jImpl % "test",
        Dependencies.log4j12Api % "test"
      ) ++ commonUnitTestDependencies
    )
}

object CelebornService {
  lazy val service = Project("celeborn-service", file("service"))
    .dependsOn(CelebornCommon.common)
    .settings (
      commonSettings,
      libraryDependencies ++= Seq(
        Dependencies.findbugsJsr305,
        Dependencies.commonsIo,
        Dependencies.ioNetty,
        Dependencies.javaxServletApi,
        Dependencies.commonsCrypto,
        Dependencies.slf4jApi,
        Dependencies.log4jSlf4jImpl % "test",
        Dependencies.log4j12Api % "test"
      ) ++ commonUnitTestDependencies
    )
}

object CelebornMaster {
  lazy val master = Project("celeborn-master", file("master"))
    .dependsOn(CelebornCommon.common, CelebornService.service)
    .settings (
      commonSettings,
      protoSettings,
      libraryDependencies ++= Seq(
        Dependencies.guava,
        Dependencies.protobufJava,
        Dependencies.ioNetty,
        Dependencies.hadoopClientApi,
        Dependencies.log4j12Api,
        Dependencies.log4jSlf4jImpl,
        Dependencies.ratisClient,
        Dependencies.ratisCommon,
        Dependencies.ratisGrpc,
        Dependencies.ratisNetty,
        Dependencies.ratisServer,
        Dependencies.ratisShell
      ) ++ commonUnitTestDependencies
    )
}

object CelebornWorker {
  lazy val worker = Project("celeborn-worker", file("worker"))
    .dependsOn(CelebornService.service)
    .dependsOn(CelebornCommon.common % "test->test;compile->compile")
    .dependsOn(CelebornClient.client % "test->compile")
    .dependsOn(CelebornMaster.master % "test->compile")
    .settings (
      commonSettings,
      excludeDependencies ++= Seq(
        // ratis-common/ratis-client are the transitive dependencies from celeborn-common
        ExclusionRule("org.apache.ratis", "ratis-common"),
        ExclusionRule("org.apache.ratis", "ratis-client")
      ),
      libraryDependencies ++= Seq(
        Dependencies.guava,
        Dependencies.commonsIo,
        Dependencies.ioNetty,
        Dependencies.log4j12Api,
        Dependencies.log4jSlf4jImpl,
        Dependencies.leveldbJniAll,
        Dependencies.roaringBitmap,
        Dependencies.rocksdbJni,
        Dependencies.scalatestMockito % "test"
      ) ++ commonUnitTestDependencies
    )
}

////////////////////////////////////////////////////////
//                   Spark Client                     //
////////////////////////////////////////////////////////

object Spark24 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-2"
  val sparkClientProjectName = "celeborn-client-spark-2"
  val sparkClientShadedProjectPath = "client-spark/spark-2-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-2-shaded"

  // val jacksonVersion = "2.5.7"
  // val jacksonDatabindVersion = "2.6.7.3"
  val lz4JavaVersion = "1.4.0"
  val sparkProjectScalaVersion = "2.11.12"
  // scalaBinaryVersion
  // val scalaBinaryVersion = "2.11"
  val sparkVersion = "2.4.8"
  val zstdJniVersion = "1.4.4-3"
}

object Spark30 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  val lz4JavaVersion = "1.7.1"
  val sparkProjectScalaVersion = "2.12.10"

  val sparkVersion = "3.0.3"
  val zstdJniVersion = "1.4.4-3"

  override val includeColumnarShuffle: Boolean = true
}

object Spark31 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  val lz4JavaVersion = "1.7.1"
  val sparkProjectScalaVersion = "2.12.10"

  val sparkVersion = "3.1.3"
  val zstdJniVersion = "1.4.8-1"

  override val includeColumnarShuffle: Boolean = true
}

object Spark32 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  val lz4JavaVersion = "1.7.1"
  val sparkProjectScalaVersion = "2.12.15"

  val sparkVersion = "3.2.4"
  val zstdJniVersion = "1.5.0-4"

  override val includeColumnarShuffle: Boolean = true
}

object Spark33 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  // val jacksonVersion = "2.13.4"
  // val jacksonDatabindVersion = "2.13.4.2"
  val lz4JavaVersion = "1.8.0"
  val sparkProjectScalaVersion = "2.12.15"
  // scalaBinaryVersion
  // val scalaBinaryVersion = "2.12"
  val sparkVersion = "3.3.3"
  val zstdJniVersion = "1.5.2-1"

  override val includeColumnarShuffle: Boolean = true
}

object Spark34 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  val lz4JavaVersion = "1.8.0"
  val sparkProjectScalaVersion = "2.12.17"

  val sparkVersion = "3.4.2"
  val zstdJniVersion = "1.5.2-5"

  override val includeColumnarShuffle: Boolean = true
}

object Spark35 extends SparkClientProjects {

  val sparkClientProjectPath = "client-spark/spark-3"
  val sparkClientProjectName = "celeborn-client-spark-3"
  val sparkClientShadedProjectPath = "client-spark/spark-3-shaded"
  val sparkClientShadedProjectName = "celeborn-client-spark-3-shaded"

  val lz4JavaVersion = "1.8.0"
  val sparkProjectScalaVersion = "2.12.18"

  val sparkVersion = "3.5.0"
  val zstdJniVersion = "1.5.5-4"
}

trait SparkClientProjects {

  val sparkClientProjectPath: String
  val sparkClientProjectName: String
  val sparkClientShadedProjectPath: String
  val sparkClientShadedProjectName: String

  val lz4JavaVersion: String
  val sparkProjectScalaVersion: String
  val sparkVersion: String
  val zstdJniVersion: String

  val includeColumnarShuffle: Boolean = false

  def modules: Seq[Project] = {
    val seq = Seq(sparkCommon, sparkClient, sparkIt, sparkGroup, sparkClientShade)
    if (includeColumnarShuffle) seq :+ sparkColumnarShuffle else seq
  }

  // for test only, don't use this group for any other projects
  lazy val sparkGroup = {
    val p = (project withId "celeborn-spark-group")
      .aggregate(sparkCommon, sparkClient, sparkIt)
    if (includeColumnarShuffle) {
      p.aggregate(sparkColumnarShuffle)
    } else {
      p
    }
  }

  def sparkCommon: Project = {
    Project("celeborn-spark-common", file("client-spark/common"))
      .dependsOn(CelebornCommon.common)
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(CelebornClient.client % "test->test;compile->compile")
      .settings (
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
          "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
          "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests"
        ) ++ commonUnitTestDependencies
      )
  }
  
  def sparkClient: Project = {
    Project(sparkClientProjectName, file(sparkClientProjectPath))
      .dependsOn(CelebornCommon.common, sparkCommon)
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(CelebornClient.client % "test->test;compile->compile")
      .settings (
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
          "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        ) ++ commonUnitTestDependencies
      )
  }

  def sparkColumnarShuffle: Project = {
    Project("celeborn-spark-3-columnar-shuffle", file("client-spark/spark-3-columnar-shuffle"))
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(sparkClient % "test->test;compile->compile")
      .dependsOn(CelebornClient.client % "test")
      .settings(
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        ) ++ commonUnitTestDependencies ++ Seq(Dependencies.mockitoInline % "test")
      )
  }

  def sparkIt: Project = {
    Project("celeborn-spark-it", file("tests/spark-it"))
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(CelebornCommon.common % "test->test;compile->compile")
      .dependsOn(CelebornClient.client % "test->test;compile->compile")
      .dependsOn(CelebornMaster.master % "test->test;compile->compile")
      .dependsOn(CelebornWorker.worker % "test->test;compile->compile")
      .dependsOn(sparkClient % "test->test;compile->compile")
      .settings (
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-core" % sparkVersion % "test",
          "org.apache.spark" %% "spark-sql" % sparkVersion % "test",
          "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
          "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests"
        ) ++ commonUnitTestDependencies
      )
  }

  def sparkClientShade: Project = {
    val p = Project(sparkClientShadedProjectName, file(sparkClientShadedProjectPath))
      .dependsOn(sparkClient)
      .disablePlugins(AddMetaInfLicenseFiles)
      .settings (
        commonSettings,
        releaseSettings,

        // align final shaded jar name with maven.
        (assembly / assemblyJarName) := {
          val extension = artifact.value.extension
          s"${moduleName.value}_${scalaBinaryVersion.value}-${version.value}.$extension"
        },
  
        (assembly / test) := { },
  
        (assembly / logLevel) := Level.Info,
  
        // Exclude `scala-library` from assembly.
        (assembly / assemblyPackageScala / assembleArtifact) := false,
  
        (assembly / assemblyExcludedJars) := {
          val cp = (assembly / fullClasspath).value
          cp filter { v =>
            val name = v.data.getName
            !(name.startsWith("celeborn-") ||
              name.startsWith("protobuf-java-") ||
              name.startsWith("guava-") ||
              name.startsWith("failureaccess-") ||
              name.startsWith("netty-") ||
              name.startsWith("commons-lang3-") ||
              name.startsWith("RoaringBitmap-"))
          }
        },
  
        (assembly / assemblyShadeRules) := Seq(
          ShadeRule.rename("com.google.protobuf.**" -> "org.apache.celeborn.shaded.com.google.protobuf.@1").inAll,
          ShadeRule.rename("com.google.common.**" -> "org.apache.celeborn.shaded.com.google.common.@1").inAll,
          ShadeRule.rename("io.netty.**" -> "org.apache.celeborn.shaded.io.netty.@1").inAll,
          ShadeRule.rename("org.apache.commons.**" -> "org.apache.celeborn.shaded.org.apache.commons.@1").inAll,
          ShadeRule.rename("org.roaringbitmap.**" -> "org.apache.celeborn.shaded.org.roaringbitmap.@1").inAll
        ),
  
        (assembly / assemblyMergeStrategy) := {
          case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
          // the LicenseAndNoticeMergeStrategy always picks the license/notice file from the current project
          case m @ ("META-INF/LICENSE" | "META-INF/NOTICE") => CustomMergeStrategy("LicenseAndNoticeMergeStrategy") { conflicts =>
            val entry = conflicts.head
            val projectLicenseFile = (Compile / resourceDirectory).value / entry.target
            val stream = () => new java.io.BufferedInputStream(new java.io.FileInputStream(projectLicenseFile))
            Right(Vector(JarEntry(entry.target, stream)))
          }
          case PathList(ps@_*) if Assembly.isLicenseFile(ps.last) => MergeStrategy.discard
          // Drop all proto files that are not needed as artifacts of the build.
          case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
          case m if m.toLowerCase(Locale.ROOT).startsWith("meta-inf/native-image") => MergeStrategy.discard
          // Drop netty jnilib
          case m if m.toLowerCase(Locale.ROOT).endsWith(".jnilib") => MergeStrategy.discard
          // rename netty native lib
          case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => CustomMergeStrategy.rename( _ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_x86_64.so" )
          case "META-INF/native/libnetty_transport_native_epoll_aarch_64.so" => CustomMergeStrategy.rename( _ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_aarch_64.so" )
          case _ => MergeStrategy.first
        },

        Compile / packageBin := assembly.value
      )
    if (includeColumnarShuffle) {
        p.dependsOn(sparkColumnarShuffle)
    } else {
        p
    }
  }
}

////////////////////////////////////////////////////////
//                   Flink Client                     //
////////////////////////////////////////////////////////

object Flink114 extends FlinkClientProjects {
  val flinkVersion = "1.14.6"

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath = "client-flink/flink-1.14"
  val flinkClientProjectName = "celeborn-client-flink-1_14"
  val flinkClientShadedProjectPath: String = "client-flink/flink-1.14-shaded"
  val flinkClientShadedProjectName: String = "celeborn-client-flink-1_14-shaded"

  override lazy val flinkStreamingDependency: ModuleID = "org.apache.flink" %% "flink-streaming-java" % flinkVersion % "test"
  override lazy val flinkClientsDependency: ModuleID = "org.apache.flink" %% "flink-clients" % flinkVersion % "test"
  override lazy val flinkRuntimeWebDependency: ModuleID = "org.apache.flink" %% "flink-runtime-web" % flinkVersion % "test"
}

object Flink115 extends FlinkClientProjects {
  val flinkVersion = "1.15.4"

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath = "client-flink/flink-1.15"
  val flinkClientProjectName = "celeborn-client-flink-1_15"
  val flinkClientShadedProjectPath: String = "client-flink/flink-1.15-shaded"
  val flinkClientShadedProjectName: String = "celeborn-client-flink-1_15-shaded"
}

object Flink117 extends FlinkClientProjects {
  val flinkVersion = "1.17.0"

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath = "client-flink/flink-1.17"
  val flinkClientProjectName = "celeborn-client-flink-1_17"
  val flinkClientShadedProjectPath: String = "client-flink/flink-1.17-shaded"
  val flinkClientShadedProjectName: String = "celeborn-client-flink-1_17-shaded"
}

object Flink118 extends FlinkClientProjects {
  val flinkVersion = "1.18.0"

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath = "client-flink/flink-1.18"
  val flinkClientProjectName = "celeborn-client-flink-1_18"
  val flinkClientShadedProjectPath: String = "client-flink/flink-1.18-shaded"
  val flinkClientShadedProjectName: String = "celeborn-client-flink-1_18-shaded"
}

trait FlinkClientProjects {

  val flinkVersion: String

  // note that SBT does not allow using the period symbol (.) in project names.
  val flinkClientProjectPath: String
  val flinkClientProjectName: String
  val flinkClientShadedProjectPath: String
  val flinkClientShadedProjectName: String

  lazy val flinkStreamingDependency: ModuleID = "org.apache.flink" % "flink-streaming-java" % flinkVersion % "test"
  lazy val flinkClientsDependency: ModuleID = "org.apache.flink" % "flink-clients" % flinkVersion % "test"
  lazy val flinkRuntimeWebDependency: ModuleID = "org.apache.flink" % "flink-runtime-web" % flinkVersion % "test"

  def modules: Seq[Project] = Seq(flinkCommon, flinkClient, flinkIt, flinkGroup, flinkClientShade)

  // for test only, don't use this group for any other projects
  lazy val flinkGroup = (project withId "celeborn-flink-group")
    .aggregate(flinkCommon, flinkClient, flinkIt)

  // get flink major version. e.g:
  //   1.17.0 -> 1.17
  //   1.15.4 -> 1.15
  //   1.14.6 -> 1.14
  lazy val flinkMajorVersion: String = flinkVersion.split("\\.").take(2).reduce(_ + "." + _)

  // the output would be something like: celeborn-client-flink-1.17-shaded_2.12-0.4.0-SNAPSHOT.jar
  def flinkClientShadeJarName(
      revision: String,
      artifact: Artifact,
      scalaBinaryVersionString: String): String =
    s"celeborn-client-flink-$flinkMajorVersion-shaded_$scalaBinaryVersionString" + "-" + revision + "." + artifact.extension

  def flinkCommon: Project = {
    Project("celeborn-flink-common", file("client-flink/common"))
      .dependsOn(CelebornCommon.common, CelebornClient.client)
      .settings (
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.flink" % "flink-runtime" % flinkVersion % "provided"
        ) ++ commonUnitTestDependencies
      )
  }

  def flinkClient: Project = {
    Project(flinkClientProjectName, file(flinkClientProjectPath))
      .dependsOn(CelebornCommon.common, CelebornClient.client, flinkCommon)
      .settings (
        commonSettings,

        moduleName := s"celeborn-client-flink-$flinkMajorVersion",

        libraryDependencies ++= Seq(
          "org.apache.flink" % "flink-runtime" % flinkVersion % "provided",
          Dependencies.log4jSlf4jImpl % "test",
          Dependencies.log4j12Api % "test"
        ) ++ commonUnitTestDependencies
      )
  }

  def flinkIt: Project = {
    Project("celeborn-flink-it", file("tests/flink-it"))
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(CelebornCommon.common % "test->test;compile->compile")
      .dependsOn(CelebornClient.client % "test->test;compile->compile")
      .dependsOn(CelebornMaster.master % "test->test;compile->compile")
      .dependsOn(CelebornWorker.worker % "test->test;compile->compile")
      .dependsOn(flinkClient % "test->test;compile->compile")
      .settings (
        commonSettings,
        libraryDependencies ++= Seq(
          "org.apache.flink" % "flink-runtime" % flinkVersion % "test",
          "org.apache.flink" %% "flink-scala" % flinkVersion % "test",
          flinkStreamingDependency,
          flinkClientsDependency,
          flinkRuntimeWebDependency
        ) ++ commonUnitTestDependencies
      )
  }

  def flinkClientShade: Project = {
    Project(flinkClientShadedProjectName, file(flinkClientShadedProjectPath))
      .dependsOn(flinkClient)
      .disablePlugins(AddMetaInfLicenseFiles)
      .settings (
        commonSettings,
        releaseSettings,

        moduleName := s"celeborn-client-flink-$flinkMajorVersion-shaded",

        (assembly / test) := { },

        (assembly / assemblyJarName) := {
            val revision: String = version.value
            val artifactValue: Artifact = artifact.value
            flinkClientShadeJarName(revision, artifactValue, scalaBinaryVersion.value)
        },
  
        (assembly / logLevel) := Level.Info,
  
        // Exclude `scala-library` from assembly.
        (assembly / assemblyPackageScala / assembleArtifact) := false,
  
        (assembly / assemblyExcludedJars) := {
          val cp = (assembly / fullClasspath).value
          cp filter { v =>
            val name = v.data.getName
            !(name.startsWith("celeborn-") ||
                name.startsWith("protobuf-java-") ||
                name.startsWith("guava-") ||
                name.startsWith("failureaccess-") ||
                name.startsWith("netty-") ||
                name.startsWith("commons-lang3-") ||
                name.startsWith("RoaringBitmap-"))
          }
        },
  
        (assembly / assemblyShadeRules) := Seq(
          ShadeRule.rename("com.google.protobuf.**" -> "org.apache.celeborn.shaded.com.google.protobuf.@1").inAll,
          ShadeRule.rename("com.google.common.**" -> "org.apache.celeborn.shaded.com.google.common.@1").inAll,
          ShadeRule.rename("io.netty.**" -> "org.apache.celeborn.shaded.io.netty.@1").inAll,
          ShadeRule.rename("org.apache.commons.**" -> "org.apache.celeborn.shaded.org.apache.commons.@1").inAll,
          ShadeRule.rename("org.roaringbitmap.**" -> "org.apache.celeborn.shaded.org.roaringbitmap.@1").inAll
        ),
  
        (assembly / assemblyMergeStrategy) := {
          case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
          // the LicenseAndNoticeMergeStrategy always picks the license/notice file from the current project
          case m @ ("META-INF/LICENSE" | "META-INF/NOTICE") => CustomMergeStrategy("LicenseAndNoticeMergeStrategy") { conflicts =>
            val entry = conflicts.head
            val projectLicenseFile = (Compile / resourceDirectory).value / entry.target
            val stream = () => new java.io.BufferedInputStream(new java.io.FileInputStream(projectLicenseFile))
            Right(Vector(JarEntry(entry.target, stream)))
          }
          case PathList(ps@_*) if Assembly.isLicenseFile(ps.last) => MergeStrategy.discard
          // Drop all proto files that are not needed as artifacts of the build.
          case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
          case m if m.toLowerCase(Locale.ROOT).startsWith("meta-inf/native-image") => MergeStrategy.discard
          // Drop netty jnilib
          case m if m.toLowerCase(Locale.ROOT).endsWith(".jnilib") => MergeStrategy.discard
          // rename netty native lib
          case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => CustomMergeStrategy.rename( _ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_x86_64.so" )
          case "META-INF/native/libnetty_transport_native_epoll_aarch_64.so" => CustomMergeStrategy.rename( _ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_aarch_64.so" )
          case _ => MergeStrategy.first
        },

        Compile / packageBin := assembly.value
      )
  }
}

////////////////////////////////////////////////////////
//                   MR Client                        //
////////////////////////////////////////////////////////
object MRClientProjects {

  def mrClient: Project = {
    Project("celeborn-client-mr", file("client-mr/mr"))
      .dependsOn(CelebornCommon.common, CelebornClient.client)
      .settings(
        commonSettings,
        libraryDependencies ++= Seq(
          Dependencies.hadoopClientApi,
          Dependencies.hadoopClientRuntime,
          Dependencies.hadoopMapreduceClientApp
        ) ++ commonUnitTestDependencies
      )
  }

  def mrIt: Project = {
    Project("celeborn-mr-it", file("tests/mr-it"))
      // ref: https://www.scala-sbt.org/1.x/docs/Multi-Project.html#Classpath+dependencies
      .dependsOn(CelebornCommon.common % "test->test;compile->compile")
      .dependsOn(CelebornClient.client % "test->test;compile->compile")
      .dependsOn(CelebornMaster.master % "test->test;compile->compile")
      .dependsOn(CelebornWorker.worker % "test->test;compile->compile")
      .dependsOn(mrClient % "test->test;compile->compile")
      .settings(
        commonSettings,
        copyDepsSettings,
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-client-minicluster" % Dependencies.hadoopVersion % "test",
          "org.apache.hadoop" % "hadoop-mapreduce-examples" % Dependencies.hadoopVersion % "test",
          "org.bouncycastle" % "bcpkix-jdk15on" % "1.68" % "test"
        ) ++ commonUnitTestDependencies
      )
  }

  def mrClientShade: Project = {
    Project("celeborn-client-mr-shaded", file("client-mr/mr-shaded"))
      .dependsOn(mrClient)
      .disablePlugins(AddMetaInfLicenseFiles)
      .settings(
        commonSettings,
        releaseSettings,

        // align final shaded jar name with maven.
        (assembly / assemblyJarName) := {
          val extension = artifact.value.extension
          s"${moduleName.value}_${scalaBinaryVersion.value}-${version.value}.$extension"
        },

        (assembly / test) := {},

        (assembly / logLevel) := Level.Info,

        // include `scala-library` from assembly.
        (assembly / assemblyPackageScala / assembleArtifact) := true,

        (assembly / assemblyExcludedJars) := {
          val cp = (assembly / fullClasspath).value
          cp filter { v =>
            val name = v.data.getName
            !(name.startsWith("celeborn-") ||
              name.startsWith("protobuf-java-") ||
              name.startsWith("guava-") ||
              name.startsWith("failureaccess-") ||
              name.startsWith("netty-") ||
              name.startsWith("commons-lang3-") ||
              name.startsWith("RoaringBitmap-") ||
              name.startsWith("lz4-java-") ||
              name.startsWith("zstd-jni-") ||
              name.startsWith("scala-library-"))
          }
        },

        (assembly / assemblyShadeRules) := Seq(
          ShadeRule.rename("com.google.protobuf.**" -> "org.apache.celeborn.shaded.com.google.protobuf.@1").inAll,
          ShadeRule.rename("com.google.common.**" -> "org.apache.celeborn.shaded.com.google.common.@1").inAll,
          ShadeRule.rename("io.netty.**" -> "org.apache.celeborn.shaded.io.netty.@1").inAll,
          ShadeRule.rename("org.apache.commons.**" -> "org.apache.celeborn.shaded.org.apache.commons.@1").inAll,
          ShadeRule.rename("org.roaringbitmap.**" -> "org.apache.celeborn.shaded.org.roaringbitmap.@1").inAll
        ),

        (assembly / assemblyMergeStrategy) := {
          case m if m.toLowerCase(Locale.ROOT).endsWith("manifest.mf") => MergeStrategy.discard
          // For netty-3.x.y.Final.jar
          case m if m.startsWith("META-INF/license/") => MergeStrategy.discard
          // the LicenseAndNoticeMergeStrategy always picks the license/notice file from the current project
          case m @ ("META-INF/LICENSE" | "META-INF/NOTICE") => CustomMergeStrategy("LicenseAndNoticeMergeStrategy") { conflicts =>
            val entry = conflicts.head
            val projectLicenseFile = (Compile / resourceDirectory).value / entry.target
            val stream = () => new java.io.BufferedInputStream(new java.io.FileInputStream(projectLicenseFile))
            Right(Vector(JarEntry(entry.target, stream)))
          }
          case PathList(ps@_*) if Assembly.isLicenseFile(ps.last) => MergeStrategy.discard
          // Drop all proto files that are not needed as artifacts of the build.
          case m if m.toLowerCase(Locale.ROOT).endsWith(".proto") => MergeStrategy.discard
          case m if m.toLowerCase(Locale.ROOT).startsWith("meta-inf/native-image") => MergeStrategy.discard
          // Drop netty jnilib
          case m if m.toLowerCase(Locale.ROOT).endsWith(".jnilib") => MergeStrategy.discard
          // rename netty native lib
          case "META-INF/native/libnetty_transport_native_epoll_x86_64.so" => CustomMergeStrategy.rename(_ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_x86_64.so")
          case "META-INF/native/libnetty_transport_native_epoll_aarch_64.so" => CustomMergeStrategy.rename(_ => "META-INF/native/liborg_apache_celeborn_shaded_netty_transport_native_epoll_aarch_64.so")
          case _ => MergeStrategy.first
        },

        Compile / packageBin := assembly.value
      )
  }

  def modules: Seq[Project] = {
    Seq(mrClient, mrIt, mrGroup, mrClientShade)
  }

  // for test only, don't use this group for any other projects
  lazy val mrGroup = (project withId "celeborn-mr-group").aggregate(mrClient, mrIt)

  val copyDeps = TaskKey[Unit]("copyDeps", "Copies needed dependencies to the build directory.")
  val destPath = (Compile / crossTarget) {
    _ / "mapreduce_lib"
  }

  lazy val copyDepsSettings = Seq(
    copyDeps := {
      val dest = destPath.value
      if (!dest.isDirectory() && !dest.mkdirs()) {
        throw new java.io.IOException("Failed to create jars directory.")
      }

      (Compile / dependencyClasspath).value.map(_.data)
        .filter { jar => jar.isFile() }
        .foreach { jar =>
          val destJar = new File(dest, jar.getName())
          if (destJar.isFile()) {
            destJar.delete()
          }
          Files.copy(jar.toPath(), destJar.toPath())
        }
    },
    (Test / compile) := {
      copyDeps.value
      (Test / compile).value
    }
  )
}
