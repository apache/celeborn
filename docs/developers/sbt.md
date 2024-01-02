---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Building via SBT

Starting from version 0.4.0, the Celeborn project supports building and packaging using SBT. This article provides a detailed guide on how to build the Celeborn project using SBT.

## System Requirements

Celeborn Service (master/worker) supports Scala 2.11/2.12/2.13 and Java 8/11/17.

The following table indicates the compatibility of Celeborn Spark and Flink clients with different versions of Spark and Flink for various Java and Scala versions:

|            | Java 8/Scala 2.11 | Java 8/Scala 2.12 | Java 11/Scala 2.12 | Java 17/Scala 2.12 | Java 8/Scala 2.13 | Java 11/Scala 2.13 | Java 17/Scala 2.13 |
|------------|-------------------|-------------------|--------------------|--------------------|-------------------|--------------------|--------------------|
| Spark 2.4  | &#10004;          | &#x274C;          | &#x274C;           | &#x274C;           | &#x274C;          | &#x274C;           | &#x274C;           |
| Spark 3.0  | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           | &#x274C;          | &#x274C;           | &#x274C;           |
| Spark 3.1  | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           | &#x274C;          | &#x274C;           | &#x274C;           |
| Spark 3.2  | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           | &#10004;          | &#10004;           | &#x274C;           |
| Spark 3.3  | &#x274C;          | &#10004;          | &#10004;           | &#10004;           | &#10004;          | &#10004;           | &#10004;           |
| Spark 3.4  | &#x274C;          | &#10004;          | &#10004;           | &#10004;           | &#10004;          | &#10004;           | &#10004;           |
| Spark 3.5  | &#x274C;          | &#10004;          | &#10004;           | &#10004;           | &#10004;          | &#10004;           | &#10004;           |
| Flink 1.14 | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           | &#x274C;          | &#x274C;           | &#x274C;           |
| Flink 1.15 | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           | &#x274C;          | &#x274C;           | &#x274C;           |
| Flink 1.17 | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           | &#x274C;          | &#x274C;           | &#x274C;           |
| Flink 1.18 | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           | &#x274C;          | &#x274C;           | &#x274C;           |

## Useful SBT commands

### Packaging the Project

As an example, one can build a version of Celeborn as follows:

```
./build/sbt clean package
```

To create a Celeborn distribution like those distributed by the [Celeborn Downloads](https://celeborn.apache.org/download/) page, and that is laid out so as to be runnable, use `./build/make-distribution.sh` in the project root directory.

```
./build/make-distribution.sh --sbt-enabled --release
```

### Maven-Style Profile Management

We have adopted the Maven-style profile management for our Client module. For example, you can enable the Spark 3.3 client module by adding `-Pspark-3.3`:

```
# ./build/sbt -Pspark-3.3 projects

[info] set current project to celeborn (in build file:/root/celeborn/)
[info] In file:/root/celeborn/
[info]   * celeborn
[info]     celeborn-client
[info]     celeborn-client-spark-3
[info]     celeborn-client-spark-3-shaded
[info]     celeborn-common
[info]     celeborn-master
[info]     celeborn-service
[info]     celeborn-spark-common
[info]     celeborn-spark-group
[info]     celeborn-spark-it
[info]     celeborn-worker
```

To enable the Flink 1.15 client module, add `-Pflink-1.15`:

```
# ./build/sbt -Pflink-1.15 projects

[info] set current project to celeborn (in build file:/root/celeborn/)
[info] In file:/root/celeborn/
[info]   * celeborn
[info]     celeborn-client
[info]     celeborn-client-flink-1_15
[info]     celeborn-client-flink-1_15-shaded
[info]     celeborn-common
[info]     celeborn-flink-common
[info]     celeborn-flink-group
[info]     celeborn-flink-it
[info]     celeborn-master
[info]     celeborn-service
[info]     celeborn-worker
```

By using these profiles, you can easily switch between different client modules for Spark and Flink. These profiles enable specific dependencies and configurations relevant to the chosen version. This way, you can conveniently manage and build the desired configurations of the Celeborn project.

### Building Spark/Flink Assembly Client Jars

For example, you can build the Spark 3.3 client assembly jar by running the following commands:

```shell
$ ./build/sbt -Pspark-3.3
> project celeborn-client-spark-3-shaded
> assembly

$ # Or, you can use sbt directly with the `-Pspark-3.3` profile:
$ ./build/sbt -Pspark-3.3 celeborn-client-spark-3-shaded/assembly
```

Similarly, you can build the Flink 1.15 client assembly jar using the following commands:

```shell
$ ./build/sbt -Pflink-1.15
> project celeborn-client-flink-1_15-shaded
> assembly

$ # Or, you can use sbt directly with the `-Pflink-1.15` profile:
$ ./build/sbt -Pflink-1.15 celeborn-client-flink-1_15-shaded/assembly
```

By executing these commands, you will create assembly jar files for the respective Spark and Flink client modules. The assembly jar bundles all the dependencies, allowing the client module to be used independently with all required dependencies included.

### Building submodules individually

For instance, you can build the Celeborn Master module using:

```
$ # sbt
$ ./build/sbt
> project celeborn-master
> package

$ # Or, you can build the celeborn-master module with sbt directly using:
$ ./build/sbt celeborn-master/package
```

## Testing with SBT

To run all tests for the Celeborn project, you can use the following command:

```shell
./build/sbt test
```

Running tests for specific versions of Spark/Flink client.

For example, to run the test cases for the Spark 3.3 client, use the following command:

```shell
$ ./build/sbt -Pspark-3.3 test

$ # only run spark client related modules tests
$ ./build/sbt -Pspark-3.3 celeborn-spark-group/test
```

Similarly, to run the test cases for the Flink 1.15 client, use the following command:

```shell
$ ./build/sbt -Pflink-1.15 test

$ # only run flink client related modules tests
$ ./build/sbt -Pflink-1.15 celeborn-flink-group/test
```

### Running Individual Tests

When developing locally, it’s often convenient to run a single test or a few tests, rather than running the entire test suite.

The fastest way to run individual tests is to use the sbt console. It’s fastest to keep a sbt console open, and use it to re-run tests as necessary. For example, to run all of the tests in a particular project, e.g., master:

```
$ ./build/sbt
> project celeborn-master
> test
```
You can run a single test suite using the `testOnly` command. For example, to run the `SlotsAllocatorSuiteJ`:

```
> testOnly org.apache.celeborn.service.deploy.master.SlotsAllocatorSuiteJ
```
The `testOnly` command accepts wildcards; e.g., you can also run the `SlotsAllocatorSuiteJ` with:

```
> testOnly *SlotsAllocatorSuiteJ
```
Or you could run all of the tests in the `master` package:

```
> testOnly org.apache.celeborn.service.deploy.master.*
```
If you’d like to run just a single Java test in the `SlotsAllocatorSuiteJ`, e.g., a test that with the name `testAllocateSlotsForSinglePartitionId`, you run the following command in the sbt console:

```
> testOnly *SlotsAllocatorSuiteJ -- *SlotsAllocatorSuiteJ.testAllocateSlotsForSinglePartitionId
```

If you’d like to run just a single Scala test in the `AppDiskUsageMetricSuite`, e.g., a test that incudes "app usage snapshot" in the name, you run the following command in the sbt console:

```
> testOnly *AppDiskUsageMetricSuite -- -z "app usage snapshot"
```

If you’d prefer, you can run all of these commands on the command line (but this will be slower than running tests using an open console). To do this, you need to surround `testOnly` and the following arguments in quotes:

```
$ ./build/sbt "celeborn-master/testOnly *AppDiskUsageMetricSuite -- -z \"app usage snapshot\""
```
For more about how to run individual tests with sbt, see the [sbt documentation](https://www.scala-sbt.org/1.x/docs/Testing.html) and [JUnit Interface](https://github.com/sbt/junit-interface/#junit-interface).

## Accelerating SBT

This section provides instructions on setting up repository mirrors or proxies for a smoother SBT experience. Depending on your location and network conditions, you can choose the appropriate approach to accelerate SBT startup and enhance dependency retrieval.

### Accelerating SBT Startup

The SBT startup process involves fetching the SBT bootstrap jar, which is typically obtained from the Maven Central Repository (https://repo1.maven.org/maven2/). If you encounter slow access to this repository or if it's inaccessible in your network environment, you can expedite the SBT startup by configuring a custom artifact repository using the `DEFAULT_ARTIFACT_REPOSITORY` environment variable.


```shell
$ # The following command fetches sbt-launch-x.y.z.jar from https://maven.aliyun.com/nexus/content/groups/public/
$ # Ensure that the URL ends with a trailing slash "/"
$ export DEFAULT_ARTIFACT_REPOSITORY=https://maven.aliyun.com/nexus/content/groups/public/
$ ./build/sbt
```

This will initiate SBT using the specified repository, allowing for faster download and startup times.

### Custom SBT Repositories

The current repositories embedded within the Celeborn project are detailed below:

```
[repositories]
  local
  mavenLocal: file://${user.home}/.m2/repository/
  local-preloaded-ivy: file:///${sbt.preloaded-${sbt.global.base-${user.home}/.sbt}/preloaded/}, [organization]/[module]/[revision]/[type]s/[artifact](-[classifier]).[ext]
  local-preloaded: file:///${sbt.preloaded-${sbt.global.base-${user.home}/.sbt}/preloaded/}
  # The system property value of `celeborn.sbt.default.artifact.repository` is
  # fetched from the environment variable `DEFAULT_ARTIFACT_REPOSITORY` and
  # assigned within the build/sbt-launch-lib.bash script.
  private: ${celeborn.sbt.default.artifact.repository-file:///dev/null}
  gcs-maven-central-mirror: https://maven-central.storage-download.googleapis.com/repos/central/data/
  maven-central
  typesafe-ivy-releases: https://repo.typesafe.com/typesafe/ivy-releases/, [organization]/[module]/[revision]/[type]s/[artifact](-[classifier]).[ext], bootOnly
  sbt-ivy-snapshots: https://repo.scala-sbt.org/scalasbt/ivy-snapshots/, [organization]/[module]/[revision]/[type]s/[artifact](-[classifier]).[ext], bootOnly
  sbt-plugin-releases: https://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/, [organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]
  bintray-typesafe-sbt-plugin-releases: https://dl.bintray.com/typesafe/sbt-plugins/, [organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext]
  bintray-spark-packages: https://dl.bintray.com/spark-packages/maven/
  typesafe-releases: https://repo.typesafe.com/typesafe/releases/
```

For numerous developers across various regions, the default repository download speeds are less than optimal. To address this concern, we have curated a selection of verified public mirror templates tailored for specific regions with a significant local developer presence. For instance, we provide the `repositories-cn.template` template for developers situated within the expanse of the Chinese mainland, and the `repositories-asia.template` template designed for developers across the Asian continent. In such cases, the following command can be employed to enhance dependency download speeds:

```
cp build/sbt-config/repositories-cn.template build/sbt-config/repositories-local
```

Furthermore, it is strongly encouraged that developers from various regions contribute templates tailored to their respective areas.

!!! note
    1. `build/sbt-config/repositories-local` takes precedence over `build/sbt-config/repositories` and is ignored by `.gitignore`.
    2. Should the environment variable `DEFAULT_ARTIFACT_REPOSITORY` be set, it attains the highest priority among non-local repositories.
    3. Repository priority is determined by the file order; repositories listed earlier possess higher precedence.

Similarly, if your objective involves compiling and packaging within an intranet environment, you can edit `build/sbt-config/repositories-local` as demonstrated below:

```
[repositories]
  local
  mavenLocal: file://${user.home}/.m2/repository/
  private: ${celeborn.sbt.default.artifact.repository-file:///dev/null}
  private-central: https://example.com/repository/maven/
  private-central-http: http://example.com/repository/maven/, allowInsecureProtocol
```

`allowInsecureProtocol` is required if you want to use a repository which only supports HTTP protocol but not HTTPS, otherwise, an error will be raised (`insecure HTTP request is unsupported`), please refer to the [sbt Launcher Configuration](https://www.scala-sbt.org/1.x/docs/Launcher-Configuration.html).

For more details on sbt repository configuration, please refer to the [SBT documentation](https://www.scala-sbt.org/1.x/docs/Proxy-Repositories.html).

## Publish

SBT supports publishing shade clients (Spark/Flink/MapReduce) to an internal Maven private repository, such as [Sonatype Nexus](https://www.sonatype.com/) or [JFrog](https://jfrog.com/help/r/jfrog-artifactory-documentation/maven-repository).

Before executing the publish command, ensure that the following environment variables are correctly set:

| Environment Variable   | Description                                                                                                                           |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| ASF_USERNAME           | Sonatype repository username                                                                                                          |
| ASF_PASSWORD           | Sonatype repository password                                                                                                          |
| SONATYPE_SNAPSHOTS_URL | Sonatype repository URL for snapshot version releases, default is "https://repository.apache.org/content/repositories/snapshots"      |
| SONATYPE_RELEASES_URL  | Sonatype repository URL for official release versions, default is "https://repository.apache.org/service/local/staging/deploy/maven2" |

For example:
```shell
export SONATYPE_SNAPSHOTS_URL=http://192.168.3.46:8081/repository/maven-snapshots/
export SONATYPE_RELEASES_URL=http://192.168.3.46:8081/repository/maven-releases/
export ASF_USERNAME=admin
export ASF_PASSWORD=123456
```

Publish the shade client for Spark 3.5:
```shell
$ ./build/sbt -Pspark-3.5 celeborn-client-spark-3-shaded/publish
```

Publish the shade client for Flink 1.18:
```shell
$ ./build/sbt -Pflink-1.18 celeborn-client-flink-1_18-shaded/publish
```

Publish the shade client for MapReduce:
```shell
$ ./build/sbt -Pmr celeborn-client-mr-shaded/publish
```

Make sure to complete the necessary build and testing before executing the publish commands.
