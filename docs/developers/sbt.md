---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---


# Building via SBT

Starting from version 0.4.0, the Celeborn project supports building and packaging using SBT. This article provides a detailed guide on how to build the Celeborn project using SBT.

# System Requirements

Celeborn Service (master/worker) supports Scala 2.11/Scala 2.12 and Java 8/11/17.

The table following indicates the compatibility of Celeborn Spark and Flink clients with different versions of Spark and Flink for various Java and Scala versions:

|                | Java 8/Scala 2.11 | Java 8/Scala 2.12 | Java 11/Scala 2.12 | Java 17/Scala 2.12 |
|----------------|-------------------|-------------------|--------------------|--------------------|
| Spark 2.4      | &#10004;          | &#x274C;          | &#x274C;           | &#x274C;           |
| Spark 3.0      | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           |
| Spark 3.1      | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           |
| Spark 3.2      | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           |
| Spark 3.3      | &#x274C;          | &#10004;          | &#10004;           | &#10004;           |
| Spark 3.4      | &#x274C;          | &#10004;          | &#10004;           | &#10004;           |
| Flink 1.14     | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           |
| Flink 1.15     | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           |
| Flink 1.17     | &#x274C;          | &#10004;          | &#10004;           | &#x274C;           |


# Setting up Private Repository Proxy

If you don't encounter any network issues, you can skip this step.

## Setting up Repositories for Chinese Developers

As a developer in China, you might face slow resource loading during the sbt startup process. To accelerate resource retrieval, you can replace the default repositories with the following configurations.

Copy the following content into `${CELEBORN_HOME}/build/sbt-config/repositories`:

```plaintext
[repositories]
  local
  mavenLocal: file://${user.home}/.m2/repository/
  # The system property value of `celeborn.sbt.default.artifact.repository` is
  # fetched from the environment variable `DEFAULT_ARTIFACT_REPOSITORY` and
  # assigned within the build/sbt-launch-lib.bash script.
  private: ${celeborn.sbt.default.artifact.repository-file:///dev/null}
  aliyun-maven: https://maven.aliyun.com/nexus/content/groups/public/
  huawei-central: https://mirrors.huaweicloud.com/repository/maven/
```

Alternatively, you can generate the `repositories-local` file directly using the following command:

```shell
cp build/sbt-config/repositories-cn.template build/sbt-config/repositories-local
```

To further accelerate the download speed of `./build/sbt-launch-x.y.z.jar`, you can set the `DEFAULT_ARTIFACT_REPOSITORY` environment variable before running `./build/sbt`:

```shell
export DEFAULT_ARTIFACT_REPOSITORY=https://mirrors.huaweicloud.com/repository/maven/ && ./build/sbt
```

## Setting up Private Repositories for Offline Build

In many cases, we need to build and use private versions within an internal network (meaning public repositories are inaccessible). To achieve this, follow the steps below:

Generate the `./build/sbt-config/repositories-local` file with the following content:

```plaintext
[repositories]
  local
  mavenLocal: file://${user.home}/.m2/repository/
  # The system property value of `celeborn.sbt.default.artifact.repository` is
  # fetched from the environment variable `DEFAULT_ARTIFACT_REPOSITORY` and
  # assigned within the build/sbt-launch-lib.bash script.
  private: ${celeborn.sbt.default.artifact.repository-file:///dev/null}
```

Set the `DEFAULT_ARTIFACT_REPOSITORY` environment variable to point to your internal repository. This is necessary to download the sbt bootstrap jar. Note that your internal repository must properly proxy the sbt plugins required by the project, or it may result in errors or failures.

Use the following command to set the environment variable and run `./build/sbt`:

```shell
export DEFAULT_ARTIFACT_REPOSITORY=https://example.com/repository/maven/ && ./build/sbt
```

> NOTE:
>   1. The `repositories-local` takes precedence over `repositories`.
>   2. The priority of repositories is based on the order in the file. If you want to prioritize a specific repository, place it higher in the file.


# Useful sbt commands

## Packaging the Project

As an example, one can build a version of Celeborn as follows:

```
./build/sbt clean package
```

To create a Celeborn distribution like those distributed by the [Celeborn Downloads](https://celeborn.apache.org/download/) page, and that is laid out so as to be runnable, use `./build/make-distribution.sh` in the project root directory.

```
./build/make-distribution.sh --sbt
```

## Maven-Style Profile Management

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

## Building Spark/Flink Assembly Client Jars

For example, you can build the Spark 3.3 client assembly jar by running the following commands:

```shell
$ ./build/sbt
> project celeborn-client-spark-3-shaded
> assembly

$ # Or, you can use sbt directly with the `-Pspark-3.3` profile:
$ ./build/sbt -Pspark-3.3 celeborn-client-spark-3-shaded/assembly
```

Similarly, you can build the Flink 1.15 client assembly jar using the following commands:

```shell
$ ./build/sbt
> project celeborn-client-flink-1_15-shaded
> assembly

$ # Oryou can use sbt directly with the `-Pflink-1.15` profile:
$ ./build/sbt -Pflink-1.15 celeborn-client-flink-1_15-shaded/assembly
```

By executing these commands, you will create assembly jar files for the respective Spark and Flink client modules. The assembly jar bundles all the dependencies, allowing the client module to be used independently with all required dependencies included.

## Building submodules individually

For instance, you can build the Celeborn Client module using:

```
$ # sbt
$ build/sbt
> project celeborn-client
> package

$ # or you can build the spark-core module with sbt directly using:
$ build/sbt celeborn-client/package
```

## Testing with SBT

To run all tests for the Celeborn project, you can use the following command:

```shell
./build/sbt test
```

Running Tests for Specific Versions of Spark/Flink Client.

For example, to run the test cases for the Spark 3.3 client only, use the following command:

```shell
$ ./build/sbt -Pspark-3.3 test

$ # only run spark client related modules tests
$ ./build/sbt -Pspark-3.3 celeborn-spark-group/test
```

Similarly, to run the test cases for the Flink 1.15 client only, use the following command:

```shell
./build/sbt -Pflink-1.15 test

$ # only run flink client related modules tests
$ ./build/sbt -Pflink-1.15 celeborn-flink-group/test
```

## Running Individual Tests

When developing locally, it’s often convenient to run a single test or a few tests, rather than running the entire test suite.

The fastest way to run individual tests is to use the sbt console. It’s fastest to keep a sbt console open, and use it to re-run tests as necessary. For example, to run all of the tests in a particular project, e.g., client:

```
$ build/sbt
> project celeborn-client
> test
```
You can run a single test suite using the testOnly command. For example, to run the `ShuffleClientSuiteJ`:

```
> testOnly org.apache.celeborn.client.ShuffleClientSuiteJ
```
The testOnly command accepts wildcards; e.g., you can also run the `ShuffleClientSuiteJ` with:

```
> testOnly *ShuffleClientSuiteJ
```
Or you could run all of the tests in the `client` package:

```
> testOnly org.apache.celeborn.client.*
```
If you’d like to run just a single Java test in the `ShuffleClientSuiteJ`, e.g., a test that with the name `testPushData`, you run the following command in the sbt console:

```
> testOnly *ShuffleClientSuiteJ -- *ShuffleClientSuiteJ.testPushData
```

If you’d like to run just a single Scala test in the `WorkerStatusTrackerSuite`, e.g., a test that incudes "handleHeartbeatResponse" in the name, you run the following command in the sbt console:

```
> testOnly *WorkerStatusTrackerSuite -- -z handleHeartbeatResponse
```

If you’d prefer, you can run all of these commands on the command line (but this will be slower than running tests using an open console). To do this, you need to surround testOnly and the following arguments in quotes:

```
$ build/sbt "celeborn-client/testOnly *WorkerStatusTrackerSuite -- -z handleHeartbeatResponse"
```
For more about how to run individual tests with sbt, see the [sbt documentation](https://www.scala-sbt.org/1.x/docs/Testing.html).
