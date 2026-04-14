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

import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.util.Random

import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ShuffleManagerSpy}
import org.apache.spark.shuffle.celeborn.ShuffleManagerSpy.OpenShuffleReaderCallback
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.containers.MinIOContainer

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.{PartitionLocation, ShuffleMode}
import org.apache.celeborn.common.protocol.StorageInfo.Type

class EvictMemoryToTieredStorageTest extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  private var container: MinIOContainer = _;
  private val seenPartitionLocationsOpenReader: CopyOnWriteArrayList[PartitionLocation] =
    new CopyOnWriteArrayList[PartitionLocation]()
  private val seenPartitionLocationsUpdateFileGroups: CopyOnWriteArrayList[PartitionLocation] =
    new CopyOnWriteArrayList[PartitionLocation]()

  override def beforeAll(): Unit = {

    if (!isS3LibraryAvailable)
      return

    container = new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z");
    container.start()

    // create bucket using Minio command line tool
    container.execInContainer(
      "mc",
      "alias",
      "set",
      "dockerminio",
      "http://minio:9000",
      container.getUserName,
      container.getPassword)
    container.execInContainer("mc", "mb", "dockerminio/sample-bucket")

    System.setProperty("aws.accessKeyId", container.getUserName)
    System.setProperty("aws.secretKey", container.getPassword)

    val s3url = container.getS3URL
    val augmentedConfiguration = Map(
      CelebornConf.ACTIVE_STORAGE_TYPES.key -> "MEMORY,S3",
      CelebornConf.WORKER_STORAGE_CREATE_FILE_POLICY.key -> "MEMORY,S3",
      CelebornConf.WORKER_STORAGE_EVICT_POLICY.key -> "MEMORY,S3",
      // note that in S3 (and Minio) you cannot upload parts smaller than 5MB, so we trigger eviction only when there
      // is enough data
      CelebornConf.WORKER_MEMORY_FILE_STORAGE_MAX_FILE_SIZE.key -> "5MB",
      "celeborn.worker.directMemoryRatioForMemoryFileStorage" -> "0.2", // this is needed to use MEMORY storage
      "celeborn.hadoop.fs.s3a.endpoint" -> s"$s3url",
      "celeborn.hadoop.fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
      "celeborn.hadoop.fs.s3a.access.key" -> container.getUserName,
      "celeborn.hadoop.fs.s3a.secret.key" -> container.getPassword,
      "celeborn.hadoop.fs.s3a.path.style.access" -> "true",
      CelebornConf.S3_DIR.key -> "s3://sample-bucket/test/celeborn",
      CelebornConf.S3_ENDPOINT_REGION.key -> "dummy-region")

    setupMiniClusterWithRandomPorts(
      masterConf = augmentedConfiguration,
      workerConf = augmentedConfiguration,
      workerNum = 1)

    interceptLocationsSeenByClient()
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
    seenPartitionLocationsOpenReader.clear()
    seenPartitionLocationsUpdateFileGroups.clear()
  }

  override def afterAll(): Unit = {
    System.clearProperty("aws.accessKeyId")
    System.clearProperty("aws.secretKey")
    if (container != null) {
      container.close()
      super.afterAll()
    }
    ShuffleManagerSpy.resetHook()
  }

  def updateSparkConfWithStorageTypes(
      sparkConf: SparkConf,
      mode: ShuffleMode,
      storageTypes: String): SparkConf = {
    val s3url = container.getS3URL
    val newConf = sparkConf
      .set("spark." + CelebornConf.ACTIVE_STORAGE_TYPES.key, storageTypes)
      .set("spark." + CelebornConf.S3_DIR.key, "s3://sample-bucket/test/celeborn")
      .set("spark." + CelebornConf.S3_ENDPOINT_REGION.key, "dummy-region")
      .set(
        "spark." + CelebornConf.SHUFFLE_COMPRESSION_CODEC.key,
        "none"
      ) // we want predictable shuffle data size
      .set("spark.celeborn.hadoop.fs.s3a.endpoint", s"$s3url")
      .set(
        "spark.celeborn.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .set("spark.celeborn.hadoop.fs.s3a.access.key", container.getUserName)
      .set("spark.celeborn.hadoop.fs.s3a.secret.key", container.getPassword)
      .set("spark.celeborn.hadoop.fs.s3a.path.style.access", "true")

    super.updateSparkConf(newConf, mode)

    sparkConf.set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.ShuffleManagerSpy")
  }

  def assumeS3LibraryIsLoaded(): Unit = {
    assume(
      isS3LibraryAvailable,
      "Skipping test because AWS Hadoop client is not in the classpath(enable with -Paws)")
  }

  test("celeborn spark integration test - only memory") {
    assumeS3LibraryIsLoaded()

    val sparkConf = new SparkConf().setAppName("celeborn-demo").setMaster("local[2]")
    val celebornSparkSession = SparkSession.builder()
      .config(updateSparkConfWithStorageTypes(sparkConf, ShuffleMode.HASH, "MEMORY"))
      .getOrCreate()
    repartition(celebornSparkSession, partitions = 1)
    // MEMORY partitions are not seen when opening the reader, but they are seen when discovering the actual locations
    validateLocationTypesSeenByClient(Type.MEMORY, 0, 2)
    celebornSparkSession.stop()
  }

  test("celeborn spark integration test - only s3") {
    assumeS3LibraryIsLoaded()

    val sparkConf = new SparkConf().setAppName("celeborn-demo").setMaster("local[2]")
    val celebornSparkSession = SparkSession.builder()
      .config(updateSparkConfWithStorageTypes(sparkConf, ShuffleMode.HASH, "S3"))
      .getOrCreate()

    repartition(celebornSparkSession, partitions = 1)
    validateLocationTypesSeenByClient(Type.S3, 2, 2)
    celebornSparkSession.stop()
  }

  test("celeborn spark integration test - memory does not evict to s3") {
    assumeS3LibraryIsLoaded()

    val sparkConf = new SparkConf().setAppName("celeborn-demo").setMaster("local[2]")
    val celebornSparkSession = SparkSession.builder()
      .config(updateSparkConfWithStorageTypes(sparkConf, ShuffleMode.HASH, "MEMORY,S3"))
      .getOrCreate()

    // little data, no eviction to s3 happens
    repartition(celebornSparkSession, partitions = 1)
    validateLocationTypesSeenByClient(Type.MEMORY, 0, 2)
    celebornSparkSession.stop()
  }

  test("celeborn spark integration test - memory evict to s3") {
    assumeS3LibraryIsLoaded()

    val sparkConf = new SparkConf().setAppName("celeborn-demo").setMaster("local[2]")
    val celebornSparkSession = SparkSession.builder()
      .config(updateSparkConfWithStorageTypes(sparkConf, ShuffleMode.HASH, "MEMORY,S3"))
      .getOrCreate()

    // we need to write enough to trigger eviction from MEMORY to S3
    // we want the partition to not fit the memory storage
    val sampleSeq: immutable.Seq[(String, Int)] = buildBigDataSet

    repartition(celebornSparkSession, sequence = sampleSeq, partitions = 1)
    validateLocationTypesSeenByClient(Type.S3, 2, 2)
    celebornSparkSession.stop()
  }

  test("celeborn spark integration test - memory evict to s3 after partition split") {
    assumeS3LibraryIsLoaded()

    val sparkConf = new SparkConf().setAppName("celeborn-demo").setMaster("local[2]")
    val celebornSparkSession = SparkSession.builder()
      .config(updateSparkConfWithStorageTypes(sparkConf, ShuffleMode.HASH, "MEMORY,S3"))
      // Set split threshold equal to WORKER_MEMORY_FILE_STORAGE_MAX_FILE_SIZE (5MB) to trigger
      // the following sequence that reproduces the production failure:
      // 1. MemoryTierWriter accumulates ~5MB → eviction → DfsTierWriter (S3) created.
      //    getDiskFileInfo() is now non-null, enabling the regular split-threshold check.
      //    (Without prior eviction to disk, getDiskFileInfo() == null and no split fires.)
      // 2. The S3 file grows past 5MB → SOFT_SPLIT response sent to the Spark client.
      // 3. ChangePartitionManager calls allocateFromCandidates, which builds the new location
      //    with type=MEMORY (storageTypes.head for "MEMORY,S3") and availableTypes=MEMORY|S3.
      // 4. The new MemoryTierWriter fills again → eviction triggered on the MEMORY-typed
      //    location → StorageManager.createDiskFile must handle type=MEMORY as a valid S3
      //    target (using availableStorageTypes) rather than rejecting it.
      .config(s"spark.${CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key}", "5MB")
      .getOrCreate()

    // 20MB covers all three phases:
    //   ~5MB to fill the first MemoryTierWriter and trigger eviction to S3,
    //   ~5MB of additional S3 writes to exceed the split threshold and fire SOFT_SPLIT,
    //   ~10MB remaining for the second MemoryTierWriter to fill and trigger the second eviction.
    val sampleSeq: immutable.Seq[(String, Int)] = buildDataSet(20 * 1024 * 1024)

    repartition(celebornSparkSession, sequence = sampleSeq, partitions = 1)

    // After splits there are more than 2 committed locations (one per epoch), so we assert
    // type and path for each rather than an exact count.
    assert(seenPartitionLocationsOpenReader.size >= 2)
    seenPartitionLocationsOpenReader.asScala.foreach(location => {
      assert(
        location.getStorageInfo.getType == Type.MEMORY || location.getStorageInfo.getType == Type.S3)
      assert(location.getStorageInfo.getFilePath == "")
    })
    assert(seenPartitionLocationsUpdateFileGroups.size >= 2)
    seenPartitionLocationsUpdateFileGroups.asScala.foreach { location =>
      if (location.getStorageInfo.getType == Type.MEMORY)
        assert(location.getStorageInfo.getFilePath == "")
      else if (location.getStorageInfo.getType == Type.S3)
        assert(location.getStorageInfo.getFilePath.startsWith("s3://"))
    }
    celebornSparkSession.stop()
  }

  test("celeborn spark integration test - push fails no way of evicting") {
    assumeS3LibraryIsLoaded()

    val sparkConf = new SparkConf().setAppName("celeborn-demo").setMaster("local[2]")
    val celebornSparkSession = SparkSession.builder()
      .config(updateSparkConfWithStorageTypes(sparkConf, ShuffleMode.HASH, "MEMORY"))
      .getOrCreate()

    val sampleSeq: immutable.Seq[(String, Int)] = buildBigDataSet

    // we want the partition to not fit the memory storage, the job fails
    assertThrows[SparkException](
      repartition(celebornSparkSession, sequence = sampleSeq, partitions = 1))

    celebornSparkSession.stop()
  }

  private def buildBigDataSet: immutable.Seq[(String, Int)] = buildDataSet(10 * 1024 * 1024)

  private def buildDataSet(sizeBytes: Int): immutable.Seq[(String, Int)] = {
    val big1KBString: String = StringUtils.repeat(' ', 1024)
    val numValues = sizeBytes / big1KBString.length
    (1 to numValues)
      .map(i => big1KBString + i) // all different keys
      .toList
      .map(v => (v.toUpperCase, Random.nextInt(12) + 1))
  }

  def interceptLocationsSeenByClient(): Unit = {
    val worker = getOneWorker()
    ShuffleManagerSpy.interceptOpenShuffleReader(
      new OpenShuffleReaderCallback {
        override def accept(
            appId: String,
            shuffleId: java.lang.Integer,
            client: ShuffleClient,
            startPartition: java.lang.Integer,
            endPartition: java.lang.Integer): Unit = {
          logInfo(
            s"Open Shuffle Reader for App $appId shuffleId $shuffleId locations ${worker.controller.partitionLocationInfo.primaryPartitionLocations}")
          val locations = worker.controller.partitionLocationInfo.primaryPartitionLocations.get(
            appId + "-" + shuffleId)
          logInfo(s"Locations on openReader $locations")
          seenPartitionLocationsOpenReader.addAll(locations.values());

          val partitionIdList = List.range(startPartition.intValue(), endPartition.intValue())
          partitionIdList.foreach(partitionId => {
            val fileGroups = client.updateFileGroup(shuffleId, partitionId)
            val locationsForPartition = fileGroups.partitionGroups.get(partitionId)
            logInfo(s"locationsForPartition $partitionId $locationsForPartition")
            seenPartitionLocationsUpdateFileGroups.addAll(locationsForPartition)
          })
        }
      },
      worker.conf)
  }

  def validateLocationTypesSeenByClient(
      storageType: Type,
      numberAtOpenReader: Int,
      numberAfterUpdateFileGroups: Int): Unit = {
    seenPartitionLocationsOpenReader.asScala.foreach(location => {
      assert(location.getStorageInfo.getType == storageType)
      // filePath is empty string for MEMORY and S3 at this stage
      assert(location.getStorageInfo.getFilePath == "")
    })
    seenPartitionLocationsUpdateFileGroups.asScala.foreach(location => {
      assert(location.getStorageInfo.getType == storageType)
      // at this stage for S3 the reader must know the URI
      if (storageType == Type.S3)
        assert(location.getStorageInfo.getFilePath startsWith "s3://")
    })
    assert(seenPartitionLocationsOpenReader.size == numberAtOpenReader)
    assert(seenPartitionLocationsUpdateFileGroups.size == numberAfterUpdateFileGroups)
  }

}
