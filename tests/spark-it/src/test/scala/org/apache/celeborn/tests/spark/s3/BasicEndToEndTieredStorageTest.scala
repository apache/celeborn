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

package org.apache.celeborn.tests.spark

import io.minio.{MakeBucketArgs, MinioClient}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.MinIOContainer

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode

class BasicEndToEndTieredStorageTest extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  var container: MinIOContainer = null;
  val skipAWSTest = !isClassPresent("org.apache.hadoop.fs.s3a.S3AFileSystem")

  def isClassPresent(className: String): Boolean = {
    try {
      Class.forName(className)
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  override def beforeAll(): Unit = {

    if (skipAWSTest)
      return

    container = new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z");
    container.start()

    val minioClient = MinioClient
      .builder()
      .endpoint(container.getS3URL)
      .credentials(container.getUserName, container.getPassword)
      .build();
    minioClient
      .makeBucket(MakeBucketArgs.builder().bucket("sample-bucket").build())

    System.setProperty("aws.accessKeyId", container.getUserName)
    System.setProperty("aws.secretKey", container.getPassword)

    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterAll(): Unit = {
    System.clearProperty("aws.accessKeyId")
    System.clearProperty("aws.secretKey")
    if (container != null) {
      container.close()
      super.afterAll()
    }
  }

  override def overrideDefaultConfiguration(conf: Map[String, String]): Map[String, String] = {
    val s3url = container.getS3URL
    val augmentedConfiguration = Map(
      CelebornConf.ACTIVE_STORAGE_TYPES.key -> "MEMORY,S3",
      CelebornConf.WORKER_STORAGE_CREATE_FILE_POLICY.key -> "MEMORY,S3",
      CelebornConf.WORKER_STORAGE_EVICT_POLICY.key -> "MEMORY|S3",
      "celeborn.hadoop.fs.s3a.endpoint" -> s"$s3url",
      "celeborn.hadoop.fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
      "celeborn.hadoop.fs.s3a.access.key" -> container.getUserName,
      "celeborn.hadoop.fs.s3a.secret.key" -> container.getPassword,
      "celeborn.hadoop.fs.s3a.path.style.access" -> "true",
      CelebornConf.S3_DIR.key -> "s3://sample-bucket/test/celeborn",
      CelebornConf.S3_ENDPOINT_REGION.key -> "dummy-region") ++
      conf
    super.overrideDefaultConfiguration(augmentedConfiguration)
  }

  override def updateSparkConf(sparkConf: SparkConf, mode: ShuffleMode): SparkConf = {
    val s3url = container.getS3URL
    val newConf = sparkConf
      .set("spark." + CelebornConf.ACTIVE_STORAGE_TYPES.key, "MEMORY,S3")
      .set("spark." + CelebornConf.S3_DIR.key, "s3://sample-bucket/test/celeborn")
      .set("spark." + CelebornConf.S3_ENDPOINT_REGION.key, "dummy-region")
      .set("spark.celeborn.hadoop.fs.s3a.endpoint", s"$s3url")
      .set(
        "spark.celeborn.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .set("spark.celeborn.hadoop.fs.s3a.access.key", container.getUserName)
      .set("spark.celeborn.hadoop.fs.s3a.secret.key", container.getPassword)
      .set("spark.celeborn.hadoop.fs.s3a.path.style.access", "true")

    super.updateSparkConf(newConf, mode)
  }

  test("celeborn spark integration test - s3") {
    assume(
      !skipAWSTest,
      "Skipping test because AWS Hadoop client is not in the classpath (enable with -Paws")

    val s3url = container.getS3URL
    log.info(s"s3url $s3url");
    val sparkConf = new SparkConf().setAppName("celeborn-demo").setMaster("local[2]")
    val celebornSparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .getOrCreate()
    groupBy(celebornSparkSession)

    celebornSparkSession.stop()
  }

}
