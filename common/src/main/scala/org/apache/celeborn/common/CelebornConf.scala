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

package org.apache.celeborn.common

import java.io.IOException
import java.util.{Collections, Locale, Collection => JCollection, HashMap => JHashMap, Map => JMap}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.celeborn.common.identity.{DefaultIdentityProvider, UserIdentifier}
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.internal.config._
import org.apache.celeborn.common.network.util.ByteUnit
import org.apache.celeborn.common.protocol.{PartitionSplitMode, PartitionType, StorageInfo}
import org.apache.celeborn.common.protocol.StorageInfo.Type
import org.apache.celeborn.common.protocol.StorageInfo.Type.{HDD, SSD}
import org.apache.celeborn.common.quota.DefaultQuotaManager
import org.apache.celeborn.common.util.Utils
import org.apache.hadoop.security.UserGroupInformation

class CelebornConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  import CelebornConf._

  /** Create a CelebornConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  @transient private lazy val reader: ConfigReader = {
    val _reader = new ConfigReader(new CelebornConfigProvider(settings))
    _reader.bindEnv(new ConfigProvider {
      override def get(key: String): Option[String] = Option(getenv(key))
    })
    _reader
  }

  private def loadFromMap(props: Map[String, String], silent: Boolean): Unit =
    settings.synchronized {
      // Load any rss.* system properties
      for ((key, value) <- props if key.startsWith("celeborn.") || key.startsWith("rss.")) {
        set(key, value, silent)
      }
      this
    }

  if (loadDefaults) {
    loadFromMap(Utils.getSystemProperties, false)
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): CelebornConf = {
    set(key, value, false)
  }

  private[celeborn] def set(key: String, value: String, silent: Boolean): CelebornConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException(s"null value for $key")
    }
    if (!silent) {
      logDeprecationWarning(key)
    }
    requireDefaultValueOfRemovedConf(key, value)
    settings.put(key, value)
    this
  }

  def set[T](entry: ConfigEntry[T], value: T): CelebornConf = {
    set(entry.key, entry.stringConverter(value))
    this
  }

  def set[T](entry: OptionalConfigEntry[T], value: T): CelebornConf = {
    set(entry.key, entry.rawStringConverter(value))
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): CelebornConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): CelebornConf = {
    requireDefaultValueOfRemovedConf(key, value)
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  def setIfMissing[T](entry: ConfigEntry[T], value: T): CelebornConf = {
    setIfMissing(entry.key, entry.stringConverter(value))
  }

  def setIfMissing[T](entry: OptionalConfigEntry[T], value: T): CelebornConf = {
    setIfMissing(entry.key, entry.rawStringConverter(value))
  }

  /** Remove a parameter from the configuration */
  def unset(key: String): CelebornConf = {
    settings.remove(key)
    this
  }

  def unset(entry: ConfigEntry[_]): CelebornConf = {
    unset(entry.key)
  }

  def clear(): Unit = {
    settings.clear()
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  def get[T](entry: ConfigEntry[T]): T = {
    entry.readFrom(reader)
  }

  /**
   * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then seconds are assumed.
   * @throws java.util.NoSuchElementException If the time parameter is not set
   * @throws NumberFormatException            If the value cannot be interpreted as seconds
   */
  def getTimeAsSeconds(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key))
  }

  /**
   * Get a time parameter as seconds, falling back to a default if not set. If no
   * suffix is provided then seconds are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as seconds
   */
  def getTimeAsSeconds(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key, defaultValue))
  }

  /**
   * Get a time parameter as milliseconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws java.util.NoSuchElementException If the time parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as milliseconds
   */
  def getTimeAsMs(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key))
  }

  /**
   * Get a time parameter as milliseconds, falling back to a default if not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as milliseconds
   */
  def getTimeAsMs(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then bytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set. If no
   * suffix is provided then bytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set.
   * @throws NumberFormatException If the value cannot be interpreted as bytes
   */
  def getSizeAsBytes(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue + "B"))
  }

  /**
   * Get a size parameter as Kibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
   */
  def getSizeAsKb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key))
  }

  /**
   * Get a size parameter as Kibibytes, falling back to a default if not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
   */
  def getSizeAsKb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Mebibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
   */
  def getSizeAsMb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key))
  }

  /**
   * Get a size parameter as Mebibytes, falling back to a default if not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
   */
  def getSizeAsMb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Gibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws java.util.NoSuchElementException If the size parameter is not set
   * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
   */
  def getSizeAsGb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key))
  }

  /**
   * Get a size parameter as Gibibytes, falling back to a default if not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
   */
  def getSizeAsGb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key, defaultValue))
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(getDeprecatedConfig(key, settings))
  }

  /** Get an optional value, applying variable substitution. */
  private[celeborn] def getWithSubstitution(key: String): Option[String] = {
    getOption(key).map(reader.substitute)
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /**
   * Get all parameters that start with `prefix`
   */
  def getAllWithPrefix(prefix: String): Array[(String, String)] = {
    getAll.filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }
  }

  /**
   * Get a parameter as an integer, falling back to a default if not set
   * @throws NumberFormatException If the value cannot be interpreted as an integer
   */
  def getInt(key: String, defaultValue: Int): Int = catchIllegalValue(key) {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a long, falling back to a default if not set
   * @throws NumberFormatException If the value cannot be interpreted as a long
   */
  def getLong(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a double, falling back to a default if not ste
   * @throws NumberFormatException If the value cannot be interpreted as a double
   */
  def getDouble(key: String, defaultValue: Double): Double = catchIllegalValue(key) {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /**
   * Get a parameter as a boolean, falling back to a default if not set
   * @throws IllegalArgumentException If the value cannot be interpreted as a boolean
   */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = catchIllegalValue(key) {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = {
    settings.containsKey(key) ||
    configsWithAlternatives.get(key).toSeq.flatten.exists { alt => contains(alt.key) }
  }

  private[celeborn] def contains(entry: ConfigEntry[_]): Boolean = contains(entry.key)

  /** Copy this object */
  override def clone: CelebornConf = {
    val cloned = new CelebornConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey, e.getValue, true)
    }
    cloned
  }

  /**
   * By using this instead of System.getenv(), environment variables can be mocked
   * in unit tests.
   */
  private[celeborn] def getenv(name: String): String = System.getenv(name)

  /**
   * Wrapper method for get() methods which require some specific value format. This catches
   * any [[NumberFormatException]] or [[IllegalArgumentException]] and re-raises it with the
   * incorrectly configured key in the exception message.
   */
  private def catchIllegalValue[T](key: String)(getValue: => T): T = {
    try {
      getValue
    } catch {
      case e: NumberFormatException =>
        // NumberFormatException doesn't have a constructor that takes a cause for some reason.
        throw new NumberFormatException(s"Illegal value for config key $key: ${e.getMessage}")
          .initCause(e)
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Illegal value for config key $key: ${e.getMessage}", e)
    }
  }

  // //////////////////////////////////////////////////////
  //                      Master                         //
  // //////////////////////////////////////////////////////

  // //////////////////////////////////////////////////////
  //                      Worker                         //
  // //////////////////////////////////////////////////////
  def workerHeartbeatTimeoutMs: Long = get(WORKER_HEARTBEAT_TIMEOUT)
  def workerReplicateThreads: Int = get(WORKER_REPLICATE_THREADS)
  def workerCommitThreads: Int = get(WORKER_COMMIT_THREADS)
  def shuffleCommitTimeout: Long = get(WORKER_SHUFFLE_COMMIT_TIMEOUT)

  // //////////////////////////////////////////////////////
  //                      Client                         //
  // //////////////////////////////////////////////////////
  def shuffleWriterMode: String = get(SHUFFLE_WRITER_MODE)
  def shuffleForceFallbackEnabled: Boolean = get(SHUFFLE_FORCE_FALLBACK_ENABLED)
  def shuffleForceFallbackPartitionThreshold: Long = get(SHUFFLE_FORCE_FALLBACK_PARTITION_THRESHOLD)
  def shuffleManagerPort: Int = get(SHUFFLE_MANAGER_PORT)
  def shuffleChunkSize: Long = get(SHUFFLE_CHUCK_SIZE)
  def registerShuffleMaxRetry: Int = get(SHUFFLE_REGISTER_MAX_RETRIES)
  def registerShuffleRetryWait: Long = get(SHUFFLE_REGISTER_RETRY_WAIT)
  def reserveSlotsMaxRetries: Int = get(RESERVE_SLOTS_MAX_RETRIES)
  def reserveSlotsRetryWait: Long = get(RESERVE_SLOTS_RETRY_WAIT)
  def rpcMaxParallelism: Int = get(CLIENT_RPC_MAX_PARALLELISM)
  def appHeartbeatTimeoutMs: Long = get(APPLICATION_HEARTBEAT_TIMEOUT)
  def appHeartbeatIntervalMs: Long = get(APPLICATION_HEARTBEAT_INTERVAL)
  def shuffleExpiredCheckIntervalMs: Long = get(SHUFFLE_EXPIRED_CHECK_INTERVAL)
  def workerExcludedCheckIntervalMs: Long = get(WORKER_EXCLUDED_INTERVAL)
  def shuffleRangeReadFilterEnabled: Boolean = get(SHUFFLE_RANGE_READ_FILTER_ENABLED)
  def shufflePartitionType: PartitionType = {
    get(SHUFFLE_PARTITION_TYPE) match {
      case "reduce" => PartitionType.REDUCE_PARTITION
      case "map" => PartitionType.MAP_PARTITION
      case "mapgroup" => PartitionType.MAPGROUP_REDUCE_PARTITION
      case unknown =>
        throw new IllegalArgumentException(s"Celeborn don't support partition type `$unknown`")
    }
  }

  // //////////////////////////////////////////////////////
  //               Shuffle Compression                   //
  // //////////////////////////////////////////////////////
  def shuffleCompressionCodec: String = get(SHUFFLE_COMPRESSION_CODEC)
  def shuffleCompressionZstdCompressLevel: Int = get(SHUFFLE_COMPRESSION_ZSTD_LEVEL)

  // //////////////////////////////////////////////////////
  //               Address && HA && RATIS                //
  // //////////////////////////////////////////////////////
  def masterEndpoints: Array[String] =
    get(MASTER_ENDPOINTS).toArray.map { endpoint =>
      Utils.parseHostPort(endpoint) match {
        case (host, 0) => s"$host:${HA_MASTER_NODE_PORT.defaultValue.get}"
        case (host, port) => s"$host:$port"
      }
    }

  def masterHost: String = get(MASTER_HOST)

  def masterPort: Int = get(MASTER_PORT)

  def haEnabled: Boolean = get(HA_ENABLED)

  def haMasterNodeId: Option[String] = get(HA_MASTER_NODE_ID)

  def haMasterNodeIds: Array[String] = {
    def extractPrefix(original: String, stop: String): String = {
      val i = original.indexOf(stop)
      assert(i >= 0, s"$original does not contain $stop")
      original.substring(0, i)
    }

    val nodeConfPrefix = extractPrefix(HA_MASTER_NODE_HOST.key, "<id>")
    getAllWithPrefix(nodeConfPrefix)
      .map(_._1)
      .map(k => extractPrefix(k, "."))
      .distinct
  }

  def haMasterNodeHost(nodeId: String): String = {
    val key = HA_MASTER_NODE_HOST.key.replace("<id>", nodeId)
    get(key, Utils.localHostName)
  }

  def haMasterNodePort(nodeId: String): Int = {
    val key = HA_MASTER_NODE_PORT.key.replace("<id>", nodeId)
    getInt(key, HA_MASTER_NODE_PORT.defaultValue.get)
  }

  def haMasterRatisHost(nodeId: String): String = {
    val key = HA_MASTER_NODE_RATIS_HOST.key.replace("<id>", nodeId)
    val fallbackKey = HA_MASTER_NODE_HOST.key.replace("<id>", nodeId)
    get(key, get(fallbackKey))
  }

  def haMasterRatisPort(nodeId: String): Int = {
    val key = HA_MASTER_NODE_RATIS_PORT.key.replace("<id>", nodeId)
    getInt(key, HA_MASTER_NODE_RATIS_PORT.defaultValue.get)
  }

  def haMasterRatisRpcType: String = get(HA_MASTER_RATIS_RPC_TYPE)
  def haMasterRatisStorageDir: String = get(HA_MASTER_RATIS_STORAGE_DIR)
  def haMasterRatisLogSegmentSizeMax: Long = get(HA_MASTER_RATIS_LOG_SEGMENT_SIZE_MAX)
  def haMasterRatisLogPreallocatedSize: Long = get(HA_MASTER_RATIS_LOG_PREALLOCATED_SIZE)
  def haMasterRatisLogAppenderQueueNumElements: Int =
    get(HA_MASTER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS)
  def haMasterRatisLogAppenderQueueBytesLimit: Long =
    get(HA_MASTER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT)
  def haMasterRatisLogPurgeGap: Int = get(HA_MASTER_RATIS_LOG_PURGE_GAP)
  def haMasterRatisRpcRequestTimeout: Long = get(HA_MASTER_RATIS_RPC_REQUEST_TIMEOUT)
  def haMasterRatisRetryCacheExpiryTime: Long = get(HA_MASTER_RATIS_SERVER_RETRY_CACHE_EXPIRY_TIME)
  def haMasterRatisRpcTimeoutMin: Long = get(HA_MASTER_RATIS_RPC_TIMEOUT_MIN)
  def haMasterRatisRpcTimeoutMax: Long = get(HA_MASTER_RATIS_RPC_TIMEOUT_MAX)
  def haMasterRatisNotificationNoLeaderTimeout: Long =
    get(HA_MASTER_RATIS_NOTIFICATION_NO_LEADER_TIMEOUT)
  def haMasterRatisRpcSlownessTimeout: Long = get(HA_MASTER_RATIS_RPC_SLOWNESS_TIMEOUT)
  def haMasterRatisRoleCheckInterval: Long = get(HA_MASTER_RATIS_ROLE_CHECK_INTERVAL)
  def haMasterRatisSnapshotAutoTriggerEnabled: Boolean =
    get(HA_MASTER_RATIS_SNAPSHOT_AUTO_TRIGGER_ENABLED)
  def haMasterRatisSnapshotAutoTriggerThreshold: Long =
    get(HA_MASTER_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD)
  def haMasterRatisSnapshotRetentionFileNum: Int = get(HA_MASTER_RATIS_SNAPSHOT_RETENTION_FILE_NUM)

  // //////////////////////////////////////////////////////
  //                 Metrics System                      //
  // //////////////////////////////////////////////////////
  def metricsSystemEnable: Boolean = get(METRICS_ENABLED)
  def metricsSampleRate: Double = get(METRICS_SAMPLE_RATE)
  def metricsSlidingWindowSize: Int = get(METRICS_SLIDING_WINDOW_SIZE)
  def metricsCollectCriticalEnabled: Boolean = get(METRICS_COLLECT_CRITICAL_ENABLED)
  def metricsCapacity: Int = get(METRICS_CAPACITY)
  def masterPrometheusMetricHost: String = get(MASTER_PROMETHEUS_HOST)
  def masterPrometheusMetricPort: Int = get(MASTER_PROMETHEUS_PORT)
  def workerPrometheusMetricHost: String = get(WORKER_PROMETHEUS_HOST)
  def workerPrometheusMetricPort: Int = get(WORKER_PROMETHEUS_PORT)

  // //////////////////////////////////////////////////////
  //                      Quota                         //
  // //////////////////////////////////////////////////////
  def clusterCheckQuotaEnabled: Boolean = get(SHUFFLE_CHECK_QUOTA_ENABLED)
  def identityProviderClass: String = get(SHUFFLE_IDENTITY_PROVIDER)
  def quotaManagerClass: String = get(SHUFFLE_QUOTA_MANAGER)
  def quotaConfigurationPath: Option[String] = get(SHUFFLE_QUOTA_CONFIGURATION_PATH)

  // //////////////////////////////////////////////////////
  //               Shuffle Client Fetch                  //
  // //////////////////////////////////////////////////////
  def fetchTimeoutMs: Long = get(FETCH_TIMEOUT)
  def fetchMaxReqsInFlight: Int = get(FETCH_MAX_REQS_IN_FLIGHT)

  // //////////////////////////////////////////////////////
  //               Shuffle Client Push                   //
  // //////////////////////////////////////////////////////
  def pushReplicateEnabled: Boolean = get(PUSH_REPLICATE_ENABLED)
  def pushBufferInitialSize: Int = get(PUSH_BUFFER_INITIAL_SIZE).toInt
  def pushBufferMaxSize: Int = get(PUSH_BUFFER_MAX_SIZE).toInt
  def pushQueueCapacity: Int = get(PUSH_QUEUE_CAPACITY)
  def pushMaxReqsInFlight: Int = get(PUSH_MAX_REQS_IN_FLIGHT)
  def pushSortMemoryThreshold: Long = get(PUSH_SORT_MEMORY_THRESHOLD)
  def pushRetryThreads: Int = get(PUSH_RETRY_THREADS)
  def pushStageEndTimeout: Long = get(PUSH_STAGE_END_TIMEOUT)
  def pushLimitInFlightTimeoutMs: Long = get(PUSH_LIMIT_IN_FLIGHT_TIMEOUT)
  def pushLimitInFlightSleepDeltaMs: Long = get(PUSH_LIMIT_IN_FLIGHT_SLEEP_INTERVAL)
  def pushSplitPartitionThreads: Int = get(PUSH_SPLIT_PARTITION_THREADS)
  def partitionSplitMode: PartitionSplitMode = {
    get(PARTITION_SPLIT_MODE) match {
      case "soft" => PartitionSplitMode.SOFT
      case "hard" => PartitionSplitMode.HARD
      case unknown =>
        throw new IllegalArgumentException(
          s"Celeborn don't support partition split mode `$unknown`")
    }
  }
  def partitionSplitThreshold: Long = get(PARTITION_SPLIT_THRESHOLD)
  def batchHandleChangePartitionEnabled: Boolean = get(BATCH_HANDLE_CHANGE_PARTITION_ENABLED)
  def batchHandleChangePartitionNumThreads: Int = get(BATCH_HANDLE_CHANGE_PARTITION_THREADS)
  def batchHandleChangePartitionRequestInterval: Long = get(BATCH_HANDLE_CHANGE_PARTITION_INTERVAL)

  // //////////////////////////////////////////////////////
  //            Graceful Shutdown & Recover              //
  // //////////////////////////////////////////////////////
  def workerGracefulShutdown: Boolean = get(WORKER_GRACEFUL_SHUTDOWN_ENABLED)
  def shutdownTimeoutMs: Long = get(WORKER_GRACEFUL_SHUTDOWN_TIMEOUT)
  def checkSlotsFinishedInterval: Long = get(WORKER_CHECK_SLOTS_FINISHED_INTERVAL)
  def checkSlotsFinishedTimeoutMs: Long = get(WORKER_CHECK_SLOTS_FINISHED_TIMEOUT)
  def workerRecoverPath: String = get(WORKER_RECOVER_PATH)
  def partitionSorterCloseAwaitTimeMs: Long = get(PARTITION_SORTER_SHUTDOWN_TIMEOUT)
  def workerFlusherShutdownTimeoutMs: Long = get(WORKER_FLUSHER_SHUTDOWN_TIMEOUT)

  // //////////////////////////////////////////////////////
  //                      Flusher                        //
  // //////////////////////////////////////////////////////
  def workerFlusherBufferSize: Long = get(WORKER_FLUSHER_BUFFER_SIZE)
  def writerCloseTimeoutMs: Long = get(WORKER_WRITER_CLOSE_TIMEOUT)
  def hddFlusherThreads: Int = get(WORKER_FLUSHER_HDD_THREADS)
  def ssdFlusherThreads: Int = get(WORKER_FLUSHER_SSD_THREADS)
  def hdfsFlusherThreads: Int = get(WORKER_FLUSHER_HDFS_THREADS)
  def avgFlushTimeSlidingWindowSize: Int = get(WORKER_FLUSHER_AVGFLUSHTIME_SLIDINGWINDOW_SIZE)
  def avgFlushTimeSlidingWindowMinCount: Int =
    get(WORKER_FLUSHER_AVGFLUSHTIME_SLIDINGWINDOW_MINCOUNT)
  def diskReserveSize: Long = get(WORKER_DISK_RESERVE_SIZE)
  def diskMonitorEnabled: Boolean = get(WORKER_DISK_MONITOR_ENABLED)
  def diskMonitorCheckList: Seq[String] = get(WORKER_DISK_MONITOR_CHECKLIST)
  def diskMonitorCheckInterval: Long = get(WORKER_DISK_MONITOR_CHECK_INTERVAL)
  def diskMonitorSysBlockDir: String = get(WORKER_DISK_MONITOR_SYS_BLOCK_DIR)
  def createWriterMaxAttempts: Int = get(WORKER_WRITER_CREATE_MAX_ATTEMPTS)
  def workerStorageBaseDirPrefix: String = get(WORKER_STORAGE_BASE_DIR_PREFIX)
  def workerStorageBaseDirNumber: Int = get(WORKER_STORAGE_BASE_DIR_COUNT)

  /**
   * @return workingDir, usable space, flusher thread count, disk type
   *         check more details at CONFIGURATION_GUIDE.md
   */
  def workerBaseDirs: Seq[(String, Long, Int, Type)] = {
    // I assume there is no disk is bigger than 1 PB in recent days.
    val defaultMaxCapacity = Utils.byteStringAsBytes("1PB")
    get(WORKER_STORAGE_DIRS).map { storageDirs: Seq[String] =>
      storageDirs.map { str =>
        var maxCapacity = defaultMaxCapacity
        var diskType = HDD
        var flushThread = -1
        val (dir, attributes) = str.split(":").toList match {
          case _dir :: tail => (_dir, tail)
          case nil => throw new IllegalArgumentException(s"Illegal storage dir: $nil")
        }
        attributes.foreach {
          case capacityStr if capacityStr.toLowerCase.startsWith("capacity=") =>
            maxCapacity = Utils.byteStringAsBytes(capacityStr.split("=")(1))
          case diskTypeStr if diskTypeStr.toLowerCase.startsWith("disktype=") =>
            diskType = Type.valueOf(diskTypeStr.split("=")(1))
            if (diskType == Type.MEMORY) {
              throw new IOException(s"Invalid diskType: $diskType")
            }
          case threadCountStr if threadCountStr.toLowerCase.startsWith("flushthread=") =>
            flushThread = threadCountStr.split("=")(1).toInt
          case illegal =>
            throw new IllegalArgumentException(s"Illegal attribute: $illegal")
        }
        if (flushThread == -1) {
          flushThread = diskType match {
            case HDD => hddFlusherThreads
            case SSD => ssdFlusherThreads
          }
        }
        (dir, maxCapacity, flushThread, diskType)
      }
    }.getOrElse {
      val prefix = workerStorageBaseDirPrefix
      val number = workerStorageBaseDirNumber
      (1 to number).map { i =>
        (s"$prefix$i", defaultMaxCapacity, hddFlusherThreads, HDD)
      }
    }
  }

  def partitionSplitMinimumSize: Long = {
    getSizeAsBytes("rss.partition.split.minimum.size", "1m")
  }

  def hdfsDir: String = {
    get(HDFS_DIR).map {
      hdfsDir =>
        if (!Utils.isHdfsPath(hdfsDir)) {
          log.error(s"${HDFS_DIR.key} configuration is wrong $hdfsDir. Disable HDFS support.")
          ""
        } else {
          hdfsDir
        }
    }.getOrElse("")
  }

  // //////////////////////////////////////////////////////
  //                 Columnar Shuffle                    //
  // //////////////////////////////////////////////////////
  def columnarShuffleEnabled: Boolean = get(COLUMNAR_SHUFFLE_ENABLED)
  def columnarShuffleBatchSize: Int = get(COLUMNAR_SHUFFLE_BATCH_SIZE)
  def columnarShuffleOffHeapEnabled: Boolean = get(COLUMNAR_SHUFFLE_OFF_HEAP_ENABLED)
  def columnarShuffleDictionaryEnabled: Boolean = get(COLUMNAR_SHUFFLE_DICTIONARY_ENCODING_ENABLED)
  def columnarShuffleDictionaryMaxFactor: Double =
    get(COLUMNAR_SHUFFLE_DICTIONARY_ENCODING_MAX_FACTOR)
}

object CelebornConf extends Logging {

  /**
   * Holds information about keys that have been deprecated and do not have a replacement.
   *
   * @param key                The deprecated key.
   * @param version            The version in which the key was deprecated.
   * @param deprecationMessage Message to include in the deprecation warning.
   */
  private case class DeprecatedConfig(
      key: String,
      version: String,
      deprecationMessage: String)

  /**
   * Information about an alternate configuration key that has been deprecated.
   *
   * @param key         The deprecated config key.
   * @param version     The version in which the key was deprecated.
   * @param translation A translation function for converting old config values into new ones.
   */
  private case class AlternateConfig(
      key: String,
      version: String,
      translation: String => String = null)

  /**
   * Holds information about keys that have been removed.
   *
   * @param key          The removed config key.
   * @param version      The version in which key was removed.
   * @param defaultValue The default config value. It can be used to notice
   *                     users that they set non-default value to an already removed config.
   * @param comment      Additional info regarding to the removed config.
   */
  case class RemovedConfig(key: String, version: String, defaultValue: String, comment: String)

  /**
   * Maps deprecated config keys to information about the deprecation.
   *
   * The extra information is logged as a warning when the config is present in the user's
   * configuration.
   */
  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig("none", "1.0", "None"))

    Map(configs.map { cfg => (cfg.key -> cfg) }: _*)
  }

  /**
   * The map contains info about removed SQL configs. Keys are SQL config names,
   * map values contain extra information like the version in which the config was removed,
   * config's default value and a comment.
   *
   * Please, add a removed configuration property here only when it affects behaviours.
   * By this, it makes migrations to new versions painless.
   */
  val removedConfigs: Map[String, RemovedConfig] = {
    val masterEndpointsTips = "The behavior is controlled by `celeborn.master.endpoints` now, " +
      "please check the documentation for details."
    val configs = Seq(
      RemovedConfig("rss.ha.master.hosts", "0.2.0", null, masterEndpointsTips),
      RemovedConfig("rss.ha.service.id", "0.2.0", "rss", "configuration key removed."),
      RemovedConfig("rss.ha.nodes.rss", "0.2.0", "1,2,3,", "configuration key removed."))
    Map(configs.map { cfg => cfg.key -> cfg }: _*)
  }

  /**
   * Maps a current config key to alternate keys that were used in previous version.
   *
   * The alternates are used in the order defined in this map. If deprecated configs are
   * present in the user's configuration, a warning is logged.
   */
  private val configsWithAlternatives = Map[String, Seq[AlternateConfig]](
    "none" -> Seq(
      AlternateConfig("none", "1.0")))

  /**
   * A view of `configsWithAlternatives` that makes it more efficient to look up deprecated
   * config keys.
   *
   * Maps the deprecated config name to a 2-tuple (new config name, alternate config info).
   */
  private val allAlternatives: Map[String, (String, AlternateConfig)] = {
    configsWithAlternatives.keys.flatMap { key =>
      configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
    }.toMap
  }

  /**
   * Looks for available deprecated keys for the given config option, and return the first
   * value available.
   */
  def getDeprecatedConfig(key: String, conf: JMap[String, String]): Option[String] = {
    configsWithAlternatives.get(key).flatMap { alts =>
      alts.collectFirst {
        case alt if conf.containsKey(alt.key) =>
          val value = conf.get(alt.key)
          if (alt.translation != null) alt.translation(value) else value
      }
    }
  }

  private def requireDefaultValueOfRemovedConf(key: String, value: String): Unit = {
    removedConfigs.get(key).foreach {
      case RemovedConfig(configName, version, defaultValue, comment) =>
        if (value != defaultValue) {
          throw new IllegalArgumentException(
            s"The config '$configName' was removed in v$version. $comment")
        }
    }
  }

  /**
   * Logs a warning message if the given config key is deprecated.
   */
  private def logDeprecationWarning(key: String): Unit = {
    deprecatedConfigs.get(key).foreach { cfg =>
      logWarning(
        s"The configuration key '$key' has been deprecated in v${cfg.version} and " +
          s"may be removed in the future. ${cfg.deprecationMessage}")
      return
    }

    allAlternatives.get(key).foreach { case (newKey, cfg) =>
      logWarning(
        s"The configuration key '$key' has been deprecated in v${cfg.version} and " +
          s"may be removed in the future. Please use the new key '$newKey' instead.")
      return
    }
  }

  private[this] val confEntriesUpdateLock = new Object

  @volatile
  private[celeborn] var confEntries: JMap[String, ConfigEntry[_]] = Collections.emptyMap()

  private def register(entry: ConfigEntry[_]): Unit = confEntriesUpdateLock.synchronized {
    require(
      !confEntries.containsKey(entry.key),
      s"Duplicate CelebornConfigEntry. ${entry.key} has been registered")
    val updatedMap = new JHashMap[String, ConfigEntry[_]](confEntries)
    updatedMap.put(entry.key, entry)
    confEntries = updatedMap
  }

  private[celeborn] def unregister(entry: ConfigEntry[_]): Unit =
    confEntriesUpdateLock.synchronized {
      val updatedMap = new JHashMap[String, ConfigEntry[_]](confEntries)
      updatedMap.remove(entry.key)
      confEntries = updatedMap
    }

  private[celeborn] def getConfigEntry(key: String): ConfigEntry[_] = {
    confEntries.get(key)
  }

  private[celeborn] def getConfigEntries: JCollection[ConfigEntry[_]] = {
    confEntries.values()
  }

  private[celeborn] def containsConfigEntry(entry: ConfigEntry[_]): Boolean = {
    getConfigEntry(entry.key) == entry
  }

  private[celeborn] def containsConfigKey(key: String): Boolean = {
    confEntries.containsKey(key)
  }

  def buildConf(key: String): ConfigBuilder = ConfigBuilder(key).onCreate(register)

  val MASTER_ENDPOINTS: ConfigEntry[Seq[String]] =
    buildConf("celeborn.master.endpoints")
      .categories("client", "worker")
      .doc("Endpoints of master nodes for celeborn client to connect, allowed pattern " +
        "is: `<host1>:<port1>[,<host2>:<port2>]*`, e.g. `clb1:9097,clb2:9098,clb3:9099`. " +
        "If the port is omitted, 9097 will be used.")
      .version("0.2.0")
      .stringConf
      .transform(_.replace("<localhost>", Utils.localHostName))
      .toSequence
      .checkValue(
        endpoints => endpoints.map(_ => Try(Utils.parseHostPort(_))).forall(_.isSuccess),
        "Allowed pattern is: `<host1>:<port1>[,<host2>:<port2>]*`")
      .createWithDefaultString(s"<localhost>:9097")

  val SHUFFLE_WRITER_MODE: ConfigEntry[String] =
    buildConf("celeborn.shuffle.writer.mode")
      .withAlternative("rss.shuffle.writer.mode")
      .categories("client")
      .doc("Celeborn supports the following kind of shuffle writers. 1. hash: hash-based shuffle writer " +
        "works fine when shuffle partition count is normal; 2. sort: sort-based shuffle writer works fine " +
        "when memory pressure is high or shuffle partition count it huge.")
      .version("0.2.0")
      .stringConf
      .transform(_.toLowerCase)
      .checkValue(v => v == "hash" || v == "sort", "invalid value, options: 'hash', 'sort'")
      .createWithDefault("hash")

  val PUSH_REPLICATE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.push.replicate.enabled")
      .withAlternative("rss.push.data.replicate")
      .categories("client")
      .version("0.2.0")
      .doc("When true, Celeborn worker will replicate shuffle data to another Celeborn worker " +
        "asynchronously to ensure the pushed shuffle data won't be lost after the node failure.")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(true)

  val PUSH_BUFFER_INITIAL_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.push.buffer.initial.size")
      .withAlternative("rss.push.data.buffer.initial.size")
      .categories("client")
      .version("0.2.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8k")

  val PUSH_BUFFER_MAX_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.push.buffer.max.size")
      .withAlternative("rss.push.data.buffer.size")
      .categories("client")
      .version("0.2.0")
      .doc("Max size of reducer partition buffer memory for shuffle hash writer. The pushed " +
        "data will be buffered in memory before sending to Celeborn worker. For performance " +
        "consideration keep this buffer size higher than 32K. Example: If reducer amount is " +
        "2000, buffer size is 64K, then each task will consume up to `64KiB * 2000 = 125MiB` " +
        "heap memory.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64k")

  val PUSH_QUEUE_CAPACITY: ConfigEntry[Int] =
    buildConf("celeborn.push.queue.capacity")
      .withAlternative("rss.push.data.queue.capacity")
      .categories("client")
      .version("0.2.0")
      .doc("Push buffer queue size for a task. The maximum memory is " +
        "`celeborn.push.buffer.max.size` * `celeborn.push.queue.capacity`, " +
        "default: 64KiB * 512 = 32MiB")
      .intConf
      .createWithDefault(512)

  val PUSH_MAX_REQS_IN_FLIGHT: ConfigEntry[Int] =
    buildConf("celeborn.push.maxReqsInFlight")
      .withAlternative("rss.push.data.maxReqsInFlight")
      .categories("client")
      .version("0.2.0")
      .doc("Amount of Netty in-flight requests. The maximum memory is " +
        "`celeborn.push.maxReqsInFlight` * `celeborn.push.buffer.max.size` * " +
        "compression ratio(1 in worst case), default: 64Kib * 32 = 2Mib")
      .intConf
      .createWithDefault(32)

  val FETCH_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.fetch.timeout")
      .withAlternative("rss.fetch.chunk.timeout")
      .categories("client")
      .version("0.2.0")
      .doc("Timeout for a task to fetch chunk.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("120s")

  val FETCH_MAX_REQS_IN_FLIGHT: ConfigEntry[Int] =
    buildConf("celeborn.fetch.maxReqsInFlight")
      .withAlternative("rss.fetch.chunk.maxReqsInFlight")
      .categories("client")
      .version("0.2.0")
      .doc("Amount of in-flight chunk fetch request.")
      .intConf
      .createWithDefault(3)

  val CLIENT_RPC_MAX_PARALLELISM: ConfigEntry[Int] =
    buildConf("celeborn.rpc.maxParallelism")
      .withAlternative("rss.rpc.max.parallelism")
      .categories("client")
      .version("0.2.0")
      .doc("Max parallelism of client on sending RPC requests.")
      .intConf
      .createWithDefault(1024)

  val APPLICATION_HEARTBEAT_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.application.heartbeat.timeout")
      .withAlternative("rss.application.timeout")
      .categories("master")
      .version("0.2.0")
      .doc("Application heartbeat timeout.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("120s")

  val APPLICATION_HEARTBEAT_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.application.heartbeatInterval")
      .withAlternative("rss.application.heartbeatInterval")
      .categories("client")
      .version("0.2.0")
      .doc("Interval for client to send heartbeat message to master.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10s")

  val SHUFFLE_EXPIRED_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.shuffle.expired.checkInterval")
      .withAlternative("rss.remove.shuffle.delay")
      .categories("client")
      .version("0.2.0")
      .doc("Interval for client to check expired shuffles.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("60s")

  val WORKER_EXCLUDED_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.excluded.checkInterval")
      .withAlternative("rss.get.blacklist.delay")
      .categories("client")
      .version("0.2.0")
      .doc("Interval for client to refresh excluded worker list.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  val SHUFFLE_CHUCK_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.shuffle.chuck.size")
      .withAlternative("rss.chunk.size")
      .categories("client", "worker")
      .version("0.2.0")
      .doc("Max chunk size of reducer's merged shuffle data. For example, if a reducer's " +
        "shuffle data is 128M and the data will need 16 fetch chunk requests to fetch.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("8m")

  val SHUFFLE_REGISTER_MAX_RETRIES: ConfigEntry[Int] =
    buildConf("celeborn.shuffle.register.maxRetries")
      .withAlternative("rss.register.shuffle.max.retry")
      .categories("client")
      .version("0.2.0")
      .doc("Max retry times for client to register shuffle.")
      .intConf
      .createWithDefault(3)

  val SHUFFLE_REGISTER_RETRY_WAIT: ConfigEntry[Long] =
    buildConf("celeborn.shuffle.register.retryWait")
      .withAlternative("rss.register.shuffle.retry.wait")
      .categories("client")
      .version("0.2.0")
      .doc("Wait time before next retry if register shuffle failed.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("3s")

  val RESERVE_SLOTS_MAX_RETRIES: ConfigEntry[Int] =
    buildConf("celeborn.slots.reserve.maxRetries")
      .withAlternative("rss.reserve.slots.max.retry")
      .categories("client")
      .version("0.2.0")
      .doc("Max retry times for client to reserve slots.")
      .intConf
      .createWithDefault(3)

  val RESERVE_SLOTS_RETRY_WAIT: ConfigEntry[Long] =
    buildConf("celeborn.slots.reserve.retryWait")
      .withAlternative("rss.reserve.slots.retry.wait")
      .categories("client")
      .version("0.2.0")
      .doc("Wait time before next retry if reserve slots failed.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("3s")

  val MASTER_HOST: ConfigEntry[String] =
    buildConf("celeborn.master.host")
      .categories("master")
      .withAlternative("rss.master.host")
      .version("0.2.0")
      .doc("Hostname for master to bind.")
      .stringConf
      .transform(_.replace("<localhost>", Utils.localHostName))
      .createWithDefaultString("<localhost>")

  val MASTER_PORT: ConfigEntry[Int] =
    buildConf("celeborn.master.port")
      .withAlternative("rss.master.port")
      .categories("master")
      .version("0.2.0")
      .doc("Port for master to bind.")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "invalid port")
      .createWithDefault(9097)

  val HA_ENABLED: ConfigEntry[Boolean] = buildConf("celeborn.ha.enabled")
    .withAlternative("rss.ha.enabled")
    .categories("master")
    .version("0.2.0")
    .doc("When true, master nodes run as Raft cluster mode.")
    .booleanConf
    .createWithDefault(false)

  val HA_MASTER_NODE_ID: OptionalConfigEntry[String] =
    buildConf("celeborn.ha.master.node.id")
      .doc("Node id for master raft cluster in HA mode, if not define, " +
        "will be inferred by hostname.")
      .version("0.2.0")
      .stringConf
      .createOptional

  val HA_MASTER_NODE_HOST: ConfigEntry[String] =
    buildConf("celeborn.ha.master.node.<id>.host")
      .categories("master")
      .doc("Host to bind of master node <id> in HA mode.")
      .version("0.2.0")
      .stringConf
      .createWithDefaultString("<required>")

  val HA_MASTER_NODE_PORT: ConfigEntry[Int] =
    buildConf("celeborn.ha.master.node.<id>.port")
      .categories("master")
      .doc("Port to bind of master node <id> in HA mode.")
      .version("0.2.0")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "invalid port")
      .createWithDefault(9097)

  val HA_MASTER_NODE_RATIS_HOST: OptionalConfigEntry[String] =
    buildConf("celeborn.ha.master.node.<id>.ratis.host")
      .internal
      .categories("master")
      .doc("Ratis host to bind of master node <id> in HA mode. If not provided, " +
        s"fallback to ${HA_MASTER_NODE_HOST.key}.")
      .version("0.2.0")
      .stringConf
      .createOptional

  val HA_MASTER_NODE_RATIS_PORT: ConfigEntry[Int] =
    buildConf("celeborn.ha.master.node.<id>.ratis.port")
      .categories("master")
      .doc("Ratis port to bind of master node <id> in HA mode.")
      .version("0.2.0")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "invalid port")
      .createWithDefault(9872)

  val HA_MASTER_RATIS_RPC_TYPE: ConfigEntry[String] =
    buildConf("celeborn.ha.master.ratis.raft.rpc.type")
      .withAlternative("rss.ha.rpc.type")
      .categories("master")
      .doc("RPC type for Ratis, available options: netty, grpc.")
      .version("0.2.0")
      .stringConf
      .transform(_.toLowerCase)
      .checkValue(v => v == "netty" || v == "grpc", "illegal value, available options: netty, grpc")
      .createWithDefault("netty")

  val HA_MASTER_RATIS_STORAGE_DIR: ConfigEntry[String] =
    buildConf("celeborn.ha.master.ratis.raft.server.storage.dir")
      .categories("master")
      .withAlternative("rss.ha.storage.dir")
      .version("0.2.0")
      .stringConf
      .createWithDefault("/tmp/ratis")

  val HA_MASTER_RATIS_LOG_SEGMENT_SIZE_MAX: ConfigEntry[Long] =
    buildConf("celeborn.ha.master.ratis.raft.server.log.segment.size.max")
      .withAlternative("rss.ha.ratis.segment.size")
      .internal
      .categories("master")
      .version("0.2.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("4MB")

  val HA_MASTER_RATIS_LOG_PREALLOCATED_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.ha.master.ratis.raft.server.log.preallocated.size")
      .withAlternative("rss.ratis.segment.preallocated.size")
      .internal
      .categories("master")
      .version("0.2.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("4MB")

  val HA_MASTER_RATIS_LOG_APPENDER_QUEUE_NUM_ELEMENTS: ConfigEntry[Int] =
    buildConf("celeborn.ha.master.ratis.raft.server.log.appender.buffer.element-limit")
      .withAlternative("rss.ratis.log.appender.queue.num-elements")
      .internal
      .categories("master")
      .version("0.2.0")
      .intConf
      .createWithDefault(1024)

  val HA_MASTER_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT: ConfigEntry[Long] =
    buildConf("celeborn.ha.master.ratis.raft.server.log.appender.buffer.byte-limit")
      .withAlternative("rss.ratis.log.appender.queue.byte-limit")
      .internal
      .categories("master")
      .version("0.2.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32MB")

  val HA_MASTER_RATIS_LOG_PURGE_GAP: ConfigEntry[Int] =
    buildConf("celeborn.ha.master.ratis.raft.server.log.purge.gap")
      .withAlternative("rss.ratis.log.purge.gap")
      .internal
      .categories("master")
      .version("0.2.0")
      .intConf
      .createWithDefault(1000000)

  val HA_MASTER_RATIS_RPC_REQUEST_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.ha.master.ratis.raft.server.rpc.request.timeout")
      .withAlternative("rss.ratis.server.request.timeout")
      .internal
      .categories("master")
      .version("0.2.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("3s")

  val HA_MASTER_RATIS_SERVER_RETRY_CACHE_EXPIRY_TIME: ConfigEntry[Long] =
    buildConf("celeborn.ha.master.ratis.raft.server.retrycache.expirytime")
      .withAlternative("rss.ratis.server.retry.cache.timeout")
      .internal
      .categories("master")
      .version("0.2.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("600s")

  val HA_MASTER_RATIS_RPC_TIMEOUT_MIN: ConfigEntry[Long] =
    buildConf("celeborn.ha.master.ratis.raft.server.rpc.timeout.min")
      .withAlternative("rss.ratis.minimum.timeout")
      .internal
      .categories("master")
      .version("0.2.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("3s")

  val HA_MASTER_RATIS_RPC_TIMEOUT_MAX: ConfigEntry[Long] =
    buildConf("celeborn.ha.master.ratis.raft.server.rpc.timeout.max")
      .internal
      .categories("master")
      .version("0.2.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("5s")

  val HA_MASTER_RATIS_NOTIFICATION_NO_LEADER_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.ha.master.ratis.raft.server.notification.no-leader.timeout")
      .internal
      .categories("master")
      .version("0.2.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("120s")

  val HA_MASTER_RATIS_RPC_SLOWNESS_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.ha.master.ratis.raft.server.rpc.slowness.timeout")
      .withAlternative("rss.ratis.server.failure.timeout.duration")
      .internal
      .categories("master")
      .version("0.2.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("120s")

  val HA_MASTER_RATIS_ROLE_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.ha.master.ratis.raft.server.role.check.interval")
      .withAlternative("rss.ratis.server.role.check.interval")
      .internal
      .categories("master")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1s")

  val HA_MASTER_RATIS_SNAPSHOT_AUTO_TRIGGER_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.ha.master.ratis.raft.server.snapshot.auto.trigger.enabled")
      .withAlternative("rss.ha.ratis.snapshot.auto.trigger.enabled")
      .internal
      .categories("master")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(true)

  val HA_MASTER_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.ha.master.ratis.raft.server.snapshot.auto.trigger.threshold")
      .withAlternative("rss.ha.ratis.snapshot.auto.trigger.threshold")
      .internal
      .categories("master")
      .version("0.2.0")
      .longConf
      .createWithDefault(200000L)

  val HA_MASTER_RATIS_SNAPSHOT_RETENTION_FILE_NUM: ConfigEntry[Int] =
    buildConf("celeborn.ha.master.ratis.raft.server.snapshot.retention.file.num")
      .withAlternative("rss.ratis.snapshot.retention.file.num")
      .internal
      .categories("master")
      .version("0.2.0")
      .intConf
      .createWithDefault(3)

  val WORKER_HEARTBEAT_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.heartbeat.timeout")
      .withAlternative("rss.worker.timeout")
      .categories("master", "worker")
      .version("0.2.0")
      .doc("Worker heartbeat timeout.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("120s")

  val WORKER_REPLICATE_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.replicate.threads")
      .withAlternative("rss.worker.replicate.numThreads")
      .categories("worker")
      .version("0.2.0")
      .doc("Thread number of worker to replicate shuffle data.")
      .intConf
      .createWithDefault(64)

  val WORKER_COMMIT_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.commit.threads")
      .withAlternative("rss.worker.asyncCommitFiles.numThreads")
      .categories("worker")
      .version("0.2.0")
      .doc("Thread number of worker to commit shuffle data files asynchronously.")
      .intConf
      .createWithDefault(32)

  val WORKER_STORAGE_DIRS: OptionalConfigEntry[Seq[String]] =
    buildConf("celeborn.worker.storage.dirs")
      .withAlternative("rss.worker.base.dirs")
      .categories("worker")
      .version("0.2.0")
      .doc("Directory list to store shuffle data. It's recommended to configure one directory " +
        "on each disk. Storage size limit can be set for each directory. For the sake of " +
        "performance, there should be no more than 2 flush threads " +
        "on the same disk partition if you are using HDD, and should be 8 or more flush threads " +
        "on the same disk partition if you are using SSD. For example: " +
        "`dir1[:capacity=][:disktype=][:flushthread=],dir2[:capacity=][:disktype=][:flushthread=]`")
      .stringConf
      .toSequence
      .createOptional

  val WORKER_STORAGE_BASE_DIR_PREFIX: ConfigEntry[String] =
    buildConf("celeborn.worker.storage.baseDir.prefix")
      .withAlternative("rss.worker.base.dir.prefix")
      .categories("worker")
      .version("0.2.0")
      .doc("Base directory for Celeborn worker to write if " +
        s"`${WORKER_STORAGE_DIRS.key}` is not set.")
      .stringConf
      .createWithDefault("/mnt/disk")

  val WORKER_STORAGE_BASE_DIR_COUNT: ConfigEntry[Int] =
    buildConf("celeborn.worker.storage.baseDir.number")
      .withAlternative("rss.worker.base.dir.number")
      .categories("worker")
      .version("0.2.0")
      .doc(s"How many directories will be used if `${WORKER_STORAGE_DIRS.key}` is not set. " +
        s"The directory name is a combination of `${WORKER_STORAGE_BASE_DIR_PREFIX.key}` " +
        "and from one(inclusive) to `celeborn.worker.storage.baseDir.number`(inclusive) " +
        "step by one.")
      .intConf
      .createWithDefault(16)

  val WORKER_FLUSHER_BUFFER_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.worker.flusher.buffer.size")
      .withAlternative("rss.worker.flush.buffer.size")
      .categories("worker")
      .version("0.2.0")
      .doc("Size of buffer used by a single flusher.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("256k")

  val WORKER_SHUFFLE_COMMIT_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.shuffle.commit.timeout")
      .withAlternative("rss.flush.timeout")
      .categories("worker")
      .doc("Timeout for a Celeborn worker to commit files of a shuffle.")
      .version("0.2.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("120s")

  val WORKER_WRITER_CLOSE_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.writer.close.timeout")
      .withAlternative("rss.filewriter.timeout")
      .categories("worker")
      .doc("Timeout for a file writer to close")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("120s")

  def appExpireDurationMs(conf: CelebornConf): Long = {
    conf.getTimeAsMs("rss.expire.nonEmptyDir.duration", "1d")
  }

  def workingDirName(conf: CelebornConf): String = {
    conf.get("rss.worker.workingDirName", "hadoop/rss-worker/shuffle_data")
  }

  val WORKER_FLUSHER_HDD_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.flusher.hdd.threads")
      .withAlternative("rss.flusher.hdd.thread.count")
      .categories("worker")
      .doc("Flusher's thread count per disk used for write data to HDD disks.")
      .version("0.2.0")
      .intConf
      .createWithDefault(1)

  val WORKER_FLUSHER_SSD_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.flusher.ssd.threads")
      .withAlternative("rss.flusher.hdd.thread.count")
      .categories("worker")
      .doc("Flusher's thread count per disk used for write data to SSD disks.")
      .version("0.2.0")
      .intConf
      .createWithDefault(8)

  val WORKER_FLUSHER_HDFS_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.worker.flusher.hdfs.threads")
      .withAlternative("rss.worker.hdfs.flusher.thread.count")
      .categories("worker")
      .doc("Flusher's thread count used for write data to HDFS.")
      .version("0.2.0")
      .intConf
      .createWithDefault(4)

  val WORKER_FLUSHER_SHUTDOWN_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.flusher.shutdownTimeout")
      .withAlternative("rss.worker.diskFlusherShutdownTimeoutMs")
      .categories("worker")
      .doc("Timeout for a flusher to shutdown.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("3s")

  val WORKER_DISK_RESERVE_SIZE: ConfigEntry[Long] =
    buildConf("celeborn.worker.disk.reserve.size")
      .withAlternative("rss.disk.minimum.reserve.size")
      .categories("worker")
      .doc("Celeborn worker reserved space for each disk.")
      .version("0.2.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("5G")

  val WORKER_FLUSHER_AVGFLUSHTIME_SLIDINGWINDOW_SIZE: ConfigEntry[Int] =
    buildConf("celeborn.worker.flusher.avgFlushTime.slidingWindow.size")
      .withAlternative("rss.flusher.avg.time.window")
      .categories("worker")
      .doc("The size of sliding windows used to calculate statistics about flushed time and count.")
      .version("0.2.0")
      .intConf
      .createWithDefault(20)

  val WORKER_FLUSHER_AVGFLUSHTIME_SLIDINGWINDOW_MINCOUNT: ConfigEntry[Int] =
    buildConf("celeborn.worker.flusher.avgFlushTime.slidingWindow.minCount")
      .withAlternative("rss.flusher.avg.time.minimum.count")
      .categories("worker")
      .doc("The minimum flush count to enter a sliding window" +
        " to calculate statistics about flushed time and count.")
      .version("0.2.0")
      .internal
      .intConf
      .createWithDefault(1000)

  /**
   * @return This configuration is a guidance for load-aware slot allocation algorithm. This value
   *         is control how many disk groups will be created.
   */
  def diskGroups(conf: CelebornConf): Int = {
    conf.getInt("rss.disk.groups", 5)
  }

  def diskGroupGradient(conf: CelebornConf): Double = {
    conf.getDouble("rss.disk.group.gradient", 0.1)
  }

  def initialPartitionSize(conf: CelebornConf): Long = {
    Utils.byteStringAsBytes(conf.get("rss.initial.partition.size", "64m"))
  }

  def minimumPartitionSizeForEstimation(conf: CelebornConf): Long = {
    Utils.byteStringAsBytes(conf.get("rss.minimum.estimate.partition.size", "8m"))
  }

  def partitionSizeUpdaterInitialDelay(conf: CelebornConf): Long = {
    Utils.timeStringAsMs(conf.get("rss.partition.size.update.initial.delay", "5m"))
  }

  def partitionSizeUpdateInterval(conf: CelebornConf): Long = {
    Utils.timeStringAsMs(conf.get("rss.partition.size.update.interval", "10m"))
  }

  val PUSH_STAGE_END_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.push.stageEnd.timeout")
      .withAlternative("rss.stage.end.timeout")
      .categories("client")
      .doc("Timeout for StageEnd.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("240s")

  val PUSH_LIMIT_IN_FLIGHT_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.push.limit.inFlight.timeout")
      .withAlternative("rss.limit.inflight.timeout")
      .categories("client")
      .doc("Timeout for netty in-flight requests to be done.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("240s")

  val PUSH_LIMIT_IN_FLIGHT_SLEEP_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.push.limit.inFlight.sleepInterval")
      .withAlternative("rss.limit.inflight.sleep.delta")
      .categories("client")
      .doc("Sleep interval when check netty in-flight requests to be done.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("50ms")

  val PUSH_SORT_MEMORY_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.push.sortMemory.threshold")
      .withAlternative("rss.sort.push.data.threshold")
      .categories("client")
      .doc("When SortBasedPusher use memory over the threshold, will trigger push data.")
      .version("0.2.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("64m")

  val PUSH_RETRY_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.push.retry.threads")
      .withAlternative("rss.pushdata.retry.thread.num")
      .categories("client")
      .doc("Thread number to process shuffle re-send push data requests.")
      .version("0.2.0")
      .intConf
      .createWithDefault(8)

  val PUSH_SPLIT_PARTITION_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.push.splitPartition.threads")
      .withAlternative("rss.client.split.pool.size")
      .categories("client")
      .doc("Thread number to process shuffle split request in shuffle client.")
      .version("0.2.0")
      .intConf
      .createWithDefault(8)

  val PARTITION_SPLIT_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.shuffle.partitionSplit.threshold")
      .withAlternative("rss.partition.split.threshold")
      .categories("client")
      .doc("Shuffle file size threshold, if file size exceeds this, trigger split.")
      .version("0.2.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1G")

  val PARTITION_SPLIT_MODE: ConfigEntry[String] =
    buildConf("celeborn.shuffle.partitionSplit.mode")
      .withAlternative("rss.partition.split.mode")
      .categories("client")
      .doc("soft: the shuffle file size might be larger than split threshold. " +
        "hard: the shuffle file size will be limited to split threshold.")
      .version("0.2.0")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValue(
        Seq("soft", "hard").contains(_),
        s"Celeborn only support partition type of (soft, hard)")
      .createWithDefault("soft")

  val BATCH_HANDLE_CHANGE_PARTITION_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.shuffle.batchHandleChangePartition.enabled")
      .withAlternative("rss.change.partition.batch.enabled")
      .categories("client")
      .doc("When true, LifecycleManager will handle change partition request in batch. " +
        "Otherwise, LifecycleManager will process the requests one by one")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(false)

  val BATCH_HANDLE_CHANGE_PARTITION_THREADS: ConfigEntry[Int] =
    buildConf("celeborn.shuffle.batchHandleChangePartition.threads")
      .withAlternative("rss.change.partition.numThreads")
      .categories("client")
      .doc("Threads number for LifecycleManager to handle change partition request in batch.")
      .version("0.2.0")
      .intConf
      .createWithDefault(8)

  val BATCH_HANDLE_CHANGE_PARTITION_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.shuffle.batchHandleChangePartition.interval")
      .withAlternative("rss.change.partition.batchInterval")
      .categories("client")
      .doc("Interval for LifecycleManager to schedule handling change partition requests in batch.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("100ms")

  def pushServerPort(conf: CelebornConf): Int = {
    conf.getInt("rss.pushserver.port", 0)
  }

  def fetchServerPort(conf: CelebornConf): Int = {
    conf.getInt("rss.fetchserver.port", 0)
  }

  def replicateServerPort(conf: CelebornConf): Int = {
    conf.getInt("rss.replicateserver.port", 0)
  }

  def registerWorkerTimeoutMs(conf: CelebornConf): Long = {
    conf.getTimeAsMs("rss.register.worker.timeout", "180s")
  }

  def masterPortMaxRetry(conf: CelebornConf): Int = {
    conf.getInt("rss.master.port.maxretry", 1)
  }

  val METRICS_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.metrics.enabled")
      .withAlternative("rss.metrics.system.enabled")
      .categories("master", "worker", "metrics")
      .doc("When true, enable metrics system.")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(true)

  val METRICS_SAMPLE_RATE: ConfigEntry[Double] =
    buildConf("celeborn.metrics.sample.rate")
      .withAlternative("rss.metrics.system.sample.rate")
      .categories("master", "worker", "metrics")
      .doc("It controls if Celeborn collect timer metrics for some operations. Its value should be in [0.0, 1.0].")
      .version("0.2.0")
      .doubleConf
      .checkValue(v => v >= 0.0 && v <= 1.0, "should be in [0.0, 1.0]")
      .createWithDefault(1.0)

  val METRICS_SLIDING_WINDOW_SIZE: ConfigEntry[Int] =
    buildConf("celeborn.metrics.timer.slidingWindow.size")
      .withAlternative("rss.metrics.system.sliding.window.size")
      .categories("master", "worker", "metrics")
      .doc("The sliding window size of timer metric.")
      .version("0.2.0")
      .intConf
      .createWithDefault(4096)

  val METRICS_COLLECT_CRITICAL_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.metrics.collectPerfCritical.enabled")
      .withAlternative("rss.metrics.system.sample.perf.critical")
      .categories("master", "worker", "metrics")
      .doc("It controls whether to collect metrics which may affect performance. When enable, Celeborn collects them.")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(false)

  val METRICS_CAPACITY: ConfigEntry[Int] =
    buildConf("celeborn.metrics.capacity")
      .withAlternative("rss.inner.metrics.size")
      .categories("master", "worker", "metrics")
      .doc("The maximum number of metrics which a source can use to generate output strings.")
      .version("0.2.0")
      .intConf
      .createWithDefault(4096)

  val MASTER_PROMETHEUS_HOST: ConfigEntry[String] =
    buildConf("celeborn.master.metrics.prometheus.host")
      .withAlternative("rss.master.prometheus.metric.host")
      .categories("master", "metrics")
      .doc("Master's Prometheus host.")
      .version("0.2.0")
      .stringConf
      .createWithDefault("0.0.0.0")

  val MASTER_PROMETHEUS_PORT: ConfigEntry[Int] =
    buildConf("celeborn.master.metrics.prometheus.port")
      .withAlternative("rss.master.prometheus.metric.port")
      .categories("master", "metrics")
      .doc("Master's Prometheus port.")
      .version("0.2.0")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "invalid port")
      .createWithDefault(9098)

  val WORKER_PROMETHEUS_HOST: ConfigEntry[String] =
    buildConf("celeborn.worker.metrics.prometheus.host")
      .withAlternative("rss.worker.prometheus.metric.host")
      .categories("worker", "metrics")
      .doc("Worker's Prometheus host.")
      .version("0.2.0")
      .stringConf
      .createWithDefault("0.0.0.0")

  val WORKER_PROMETHEUS_PORT: ConfigEntry[Int] =
    buildConf("celeborn.worker.metrics.prometheus.port")
      .withAlternative("rss.worker.prometheus.metric.port")
      .categories("worker", "metrics")
      .doc("Worker's Prometheus port.")
      .version("0.2.0")
      .intConf
      .checkValue(p => p >= 1024 && p < 65535, "invalid port")
      .createWithDefault(9096)

  val SHUFFLE_CHECK_QUOTA_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.shuffle.checkQuota.enabled")
      .withAlternative("rss.cluster.checkQuota.enabled")
      .categories("quota")
      .doc("When true, before registering shuffle, LifecycleManager should check " +
        "if current user have enough quota space, if cluster don't have enough " +
        "quota space for current user, fallback to Spark's default shuffle")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(true)

  val SHUFFLE_IDENTITY_PROVIDER: ConfigEntry[String] =
    buildConf("celeborn.shuffle.identity.provider")
      .withAlternative("rss.identity.provider")
      .categories("quota")
      .doc(s"IdentityProvider class name. Default class is " +
        s"`${classOf[DefaultIdentityProvider].getName}`, return `${classOf[UserIdentifier].getName}` " +
        s"with default tenant id and username from `${classOf[UserGroupInformation].getName}`. ")
      .version("0.2.0")
      .stringConf
      .createWithDefault(classOf[DefaultIdentityProvider].getName)

  val SHUFFLE_QUOTA_MANAGER: ConfigEntry[String] =
    buildConf("celeborn.shuffle.quota.manager")
      .withAlternative("rss.quota.manager")
      .categories("quota")
      .doc(s"QuotaManger class name. Default class is `${classOf[DefaultQuotaManager].getName}`.")
      .version("0.2.0")
      .stringConf
      .createWithDefault(classOf[DefaultQuotaManager].getName)

  val SHUFFLE_QUOTA_CONFIGURATION_PATH: OptionalConfigEntry[String] =
    buildConf("celeborn.shuffle.quota.configuration.path")
      .withAlternative("rss.quota.configuration.path")
      .categories("quota")
      .doc("Quota configuration file path.")
      .version("0.2.0")
      .stringConf
      .createOptional

  def workerRPCPort(conf: CelebornConf): Int = {
    conf.getInt("rss.worker.rpc.port", 0)
  }

  def offerSlotsExtraSize(conf: CelebornConf): Int = {
    conf.getInt("rss.offer.slots.extra.size", 2)
  }

  def closeIdleConnections(conf: CelebornConf): Boolean = {
    conf.getBoolean("rss.worker.closeIdleConnections", defaultValue = false)
  }

  def replicateFastFailDurationMs(conf: CelebornConf): Long = {
    conf.getTimeAsMs("rss.replicate.fastfail.duration", "60s")
  }

  val SHUFFLE_FORCE_FALLBACK_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.shuffle.forceFallback.enabled")
      .withAlternative("rss.force.fallback")
      .categories("client")
      .version("0.2.0")
      .doc("Whether force fallback shuffle to Spark's default.")
      .booleanConf
      .createWithDefault(false)

  val SHUFFLE_FORCE_FALLBACK_PARTITION_THRESHOLD: ConfigEntry[Long] =
    buildConf("celeborn.shuffle.forceFallback.numPartitionsThreshold")
      .withAlternative("rss.max.partition.number")
      .categories("client")
      .version("0.2.0")
      .doc(
        "Celeborn will only accept shuffle of partition number lower than this configuration value.")
      .longConf
      .createWithDefault(500000)

  val SHUFFLE_MANAGER_PORT: ConfigEntry[Int] =
    buildConf("celeborn.shuffle.manager.port")
      .withAlternative("rss.driver.metaService.port")
      .categories("client")
      .version("0.2.0")
      .doc("Port used by the LifecycleManager on the Driver.")
      .intConf
      .checkValue(
        (port: Int) => {
          if (port != 0) {
            logWarning(
              "The user specifies the port used by the LifecycleManager on the Driver, and its" +
                s" values is $port, which may cause port conflicts and startup failure.")
          }
          true
        },
        "")
      .createWithDefault(0)

  val WORKER_DISK_MONITOR_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.monitor.disk.enabled")
      .withAlternative("rss.device.monitor.enabled")
      .categories("worker")
      .version("0.2.0")
      .doc("When true, worker will monitor device and report to master.")
      .booleanConf
      .createWithDefault(true)

  val WORKER_DISK_MONITOR_CHECKLIST: ConfigEntry[Seq[String]] =
    buildConf("celeborn.worker.monitor.disk.checklist")
      .withAlternative("rss.device.monitor.checklist")
      .categories("worker")
      .version("0.2.0")
      .doc("Monitor type for disk, available items are: " +
        "iohang, readwrite and diskusage.")
      .stringConf
      .transform(_.toLowerCase)
      .toSequence
      .createWithDefaultString("readwrite,diskusage")

  val WORKER_DISK_MONITOR_CHECK_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.monitor.disk.checkInterval")
      .withAlternative("rss.disk.check.interval")
      .categories("worker")
      .version("0.2.0")
      .doc("Intervals between device monitor to check disk.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("60s")

  val WORKER_DISK_MONITOR_SYS_BLOCK_DIR: ConfigEntry[String] =
    buildConf("celeborn.worker.monitor.disk.sys.block.dir")
      .withAlternative("rss.sys.block.dir")
      .categories("worker")
      .version("0.2.0")
      .doc("The directory where linux file block information is stored.")
      .stringConf
      .createWithDefault("/sys/block")

  val WORKER_WRITER_CREATE_MAX_ATTEMPTS: ConfigEntry[Int] =
    buildConf("celeborn.worker.writer.create.maxAttempts")
      .withAlternative("rss.create.file.writer.retry.count")
      .categories("worker")
      .version("0.2.0")
      .doc("Retry count for a file writer to create if its creation was failed.")
      .intConf
      .createWithDefault(3)

  def workerStatusCheckTimeout(conf: CelebornConf): Long = {
    conf.getTimeAsSeconds("rss.worker.status.check.timeout", "30s")
  }

  def checkFileCleanRetryTimes(conf: CelebornConf): Int = {
    conf.getInt("rss.worker.checkFileCleanRetryTimes", 3)
  }

  def checkFileCleanTimeoutMs(conf: CelebornConf): Long = {
    conf.getTimeAsMs("rss.worker.checkFileCleanTimeoutMs", "1000ms")
  }

  def haClientMaxTries(conf: CelebornConf): Int = {
    conf.getInt("rss.ha.client.maxTries", 15)
  }

  val SHUFFLE_PARTITION_TYPE: ConfigEntry[String] =
    buildConf("celeborn.shuffle.partition.type")
      .withAlternative("rss.partition.type")
      .categories("client")
      .doc("Type of shuffle's partition.")
      .version("0.2.0")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValue(
        Seq("reduce", "map", "mapgroup").contains(_),
        s"Celeborn only support partition type of (reduce, map, mapgroup)")
      .createWithDefault("reduce")

  val SHUFFLE_COMPRESSION_CODEC: ConfigEntry[String] =
    buildConf("celeborn.shuffle.compression.codec")
      .withAlternative("rss.client.compression.codec")
      .categories("client")
      .doc("The codec used to compress shuffle data. By default, Celeborn provides two codecs: `lz4` and `zstd`.")
      .version("0.2.0")
      .stringConf
      .transform(_.toLowerCase(Locale.ROOT))
      .checkValue(
        value => Seq("lz4", "zstd").contains(value),
        s"Invalid compression codec, Celeborn only support compression codec of (lz4, zstd).")
      .createWithDefault("lz4")

  val SHUFFLE_COMPRESSION_ZSTD_LEVEL: ConfigEntry[Int] =
    buildConf("celeborn.shuffle.compression.zstd.level")
      .withAlternative("rss.client.compression.zstd.level")
      .categories("client")
      .doc("Compression level for Zstd compression codec, its value should be an integer " +
        "between -5 and 22. Increasing the compression level will result in better compression " +
        "at the expense of more CPU and memory.")
      .version("0.2.0")
      .intConf
      .checkValue(
        value => value >= -5 && value <= 22,
        s"Compression level for Zstd compression codec should be an integer between -5 and 22.")
      .createWithDefault(1)

  def partitionSortTimeout(conf: CelebornConf): Long = {
    conf.getTimeAsMs("rss.partition.sort.timeout", "220s")
  }

  def partitionSortMaxMemoryRatio(conf: CelebornConf): Double = {
    conf.getDouble("rss.partition.sort.memory.max.ratio", 0.1)
  }

  def workerPausePushDataRatio(conf: CelebornConf): Double = {
    conf.getDouble("rss.pause.pushdata.memory.ratio", 0.85)
  }

  def workerPauseRepcaliteRatio(conf: CelebornConf): Double = {
    conf.getDouble("rss.pause.replicate.memory.ratio", 0.95)
  }

  def workerResumeRatio(conf: CelebornConf): Double = {
    conf.getDouble("rss.resume.memory.ratio", 0.5)
  }

  def initialReserveSingleSortMemory(conf: CelebornConf): Long = {
    conf.getSizeAsBytes("rss.worker.initialReserveSingleSortMemory", "1mb")
  }

  def workerDirectMemoryPressureCheckIntervalMs(conf: CelebornConf): Int = {
    conf.getInt("rss.worker.memory.check.interval", 10)
  }

  def workerDirectMemoryReportIntervalSecond(conf: CelebornConf): Int = {
    Utils.timeStringAsSeconds(conf.get("rss.worker.memory.report.interval", "10s")).toInt
  }

  def defaultStorageType(conf: CelebornConf): StorageInfo.Type = {
    val default = StorageInfo.Type.MEMORY
    val hintStr = conf.get("rss.storage.type", "memory").toUpperCase
    if (StorageInfo.Type.values().mkString.toUpperCase.contains(hintStr)) {
      logWarning(s"storage hint is invalid ${hintStr}")
      StorageInfo.Type.valueOf(hintStr)
    } else {
      default
    }
  }

  val WORKER_GRACEFUL_SHUTDOWN_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.worker.graceful.shutdown.enabled")
      .withAlternative("rss.worker.graceful.shutdown")
      .categories("worker")
      .doc("When true, during worker shutdown, the worker will wait for all released slots " +
        s"to be committed or destroyed.")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(false)

  val WORKER_GRACEFUL_SHUTDOWN_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.graceful.shutdown.timeout")
      .withAlternative("rss.worker.shutdown.timeout")
      .categories("worker")
      .doc("The worker's graceful shutdown timeout time.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("600s")

  val WORKER_CHECK_SLOTS_FINISHED_INTERVAL: ConfigEntry[Long] =
    buildConf("celeborn.worker.graceful.shutdown.checkSlotsFinished.interval")
      .withAlternative("rss.worker.checkSlots.interval")
      .categories("worker")
      .doc("The wait interval of checking whether all released slots " +
        "to be committed or destroyed during worker graceful shutdown")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1s")

  val WORKER_CHECK_SLOTS_FINISHED_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.graceful.shutdown.checkSlotsFinished.timeout")
      .withAlternative("rss.worker.checkSlots.timeout")
      .categories("worker")
      .doc("The wait time of waiting for the released slots" +
        " to be committed or destroyed during worker graceful shutdown.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("480s")

  val WORKER_RECOVER_PATH: ConfigEntry[String] =
    buildConf("celeborn.worker.graceful.shutdown.recoverPath")
      .withAlternative("rss.worker.recoverPath")
      .categories("worker")
      .doc("The path to store levelDB.")
      .version("0.2.0")
      .stringConf
      .transform(_.replace("<tmp>", System.getProperty("java.io.tmpdir")))
      .createWithDefault(s"<tmp>/recover")

  val PARTITION_SORTER_SHUTDOWN_TIMEOUT: ConfigEntry[Long] =
    buildConf("celeborn.worker.graceful.shutdown.partitionSorter.shutdownTimeout")
      .withAlternative("rss.worker.partitionSorterCloseAwaitTime")
      .categories("worker")
      .doc("The wait time of waiting for sorting partition files" +
        " during worker graceful shutdown.")
      .version("0.2.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("120s")

  def offerSlotsAlgorithm(conf: CelebornConf): String = {
    var algorithm = conf.get("rss.offer.slots.algorithm", "roundrobin")
    if (algorithm != "loadaware" && algorithm != "roundrobin") {
      logWarning(s"Config rss.offer.slots.algorithm is wrong ${algorithm}." +
        s" Use default roundrobin")
      algorithm = "roundrobin"
    }
    algorithm
  }

  val HDFS_DIR: OptionalConfigEntry[String] =
    buildConf("celeborn.storage.hdfs.dir")
      .withAlternative("rss.worker.hdfs.dir")
      .categories("worker", "client")
      .version("0.2.0")
      .doc("HDFS dir configuration for Celeborn to access HDFS.")
      .stringConf
      .createOptional

  val SHUFFLE_RANGE_READ_FILTER_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.shuffle.rangeReadFilter.enabled")
      .withAlternative("rss.range.read.filter.enabled")
      .categories("client")
      .version("0.2.0")
      .doc("If a spark application have skewed partition, this value can set to true to improve performance.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SHUFFLE_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.columnar.shuffle.enabled")
      .categories("columnar-shuffle")
      .version("0.2.0")
      .doc("Whether to enable columnar-based shuffle.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SHUFFLE_BATCH_SIZE: ConfigEntry[Int] =
    buildConf("celeborn.columnar.shuffle.batch.size")
      .categories("columnar-shuffle")
      .version("0.2.0")
      .doc("Vector batch size for columnar shuffle.")
      .intConf
      .checkValue(v => v > 0, "value must be positive")
      .createWithDefault(10000)

  val COLUMNAR_SHUFFLE_OFF_HEAP_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.columnar.offHeap.enabled")
      .categories("columnar-shuffle")
      .version("0.2.0")
      .doc("Whether to use off heap columnar vector.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SHUFFLE_DICTIONARY_ENCODING_ENABLED: ConfigEntry[Boolean] =
    buildConf("celeborn.columnar.shuffle.encoding.dictionary.enabled")
      .categories("columnar-shuffle")
      .version("0.2.0")
      .doc("Whether to use dictionary encoding for columnar-based shuffle data.")
      .booleanConf
      .createWithDefault(false)

  val COLUMNAR_SHUFFLE_DICTIONARY_ENCODING_MAX_FACTOR: ConfigEntry[Double] =
    buildConf("celeborn.columnar.shuffle.encoding.dictionary.maxFactor")
      .categories("columnar-shuffle")
      .version("0.2.0")
      .doc("Max factor for dictionary size. The max dictionary size is " +
        s"`min(${Utils.bytesToString(Short.MaxValue)}, ${COLUMNAR_SHUFFLE_BATCH_SIZE.key} * " +
        s"celeborn.columnar.shuffle.encoding.dictionary.maxFactor)`.")
      .doubleConf
      .createWithDefault(0.3)
}
