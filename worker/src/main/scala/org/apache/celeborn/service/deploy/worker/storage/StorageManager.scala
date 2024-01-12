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

package org.apache.celeborn.service.deploy.worker.storage

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, Files, Paths}
import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.{BiConsumer, IntUnaryOperator}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import io.netty.buffer.PooledByteBufAllocator
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DeviceInfo, DiskFileInfo, DiskInfo, DiskStatus, FileInfo, MapFileMeta, ReduceFileMeta, TimeWindow}
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.network.util.{NettyUtils, TransportConf}
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.util.{CelebornExitKind, CelebornHadoopUtils, JavaUtils, PbSerDeUtils, ThreadUtils, Utils}
import org.apache.celeborn.service.deploy.worker._
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager.MemoryPressureListener
import org.apache.celeborn.service.deploy.worker.shuffledb.{DB, DBBackend, DBProvider}
import org.apache.celeborn.service.deploy.worker.storage.StorageManager.hadoopFs

final private[worker] class StorageManager(conf: CelebornConf, workerSource: AbstractSource)
  extends ShuffleRecoverHelper with DeviceObserver with Logging with MemoryPressureListener {
  // mount point -> file writer
  val workingDirWriters =
    JavaUtils.newConcurrentHashMap[File, ConcurrentHashMap[String, PartitionDataWriter]]()

  val hasHDFSStorage = conf.hasHDFSStorage

  val storageExpireDirTimeout = conf.workerStorageExpireDirTimeout

  // (deviceName -> deviceInfo) and (mount point -> diskInfo)
  val (deviceInfos, diskInfos) = {
    val workingDirInfos =
      conf.workerBaseDirs.map { case (workdir, maxSpace, flusherThread, storageType) =>
        (new File(workdir, conf.workerWorkingDir), maxSpace, flusherThread, storageType)
      }

    if (workingDirInfos.size <= 0 && !hasHDFSStorage) {
      throw new IOException("Empty working directory configuration!")
    }

    DeviceInfo.getDeviceAndDiskInfos(workingDirInfos, conf)
  }
  val mountPoints = new util.HashSet[String](diskInfos.keySet())
  val hdfsDiskInfo =
    if (conf.hasHDFSStorage)
      Option(new DiskInfo("HDFS", Long.MaxValue, 999999, 999999, 0, StorageInfo.Type.HDFS))
    else None

  def disksSnapshot(): List[DiskInfo] = {
    diskInfos.synchronized {
      val disks = new util.ArrayList[DiskInfo](diskInfos.values())
      disks.asScala.toList
    }
  }

  def healthyWorkingDirs(): List[File] =
    disksSnapshot().filter(_.status == DiskStatus.HEALTHY).flatMap(_.dirs)

  private val diskOperators: ConcurrentHashMap[String, ThreadPoolExecutor] = {
    val cleaners = JavaUtils.newConcurrentHashMap[String, ThreadPoolExecutor]()
    disksSnapshot().foreach {
      diskInfo =>
        cleaners.put(
          diskInfo.mountPoint,
          ThreadUtils.newDaemonCachedThreadPool(
            s"disk-cleaner-${diskInfo.mountPoint}",
            conf.workerDiskCleanThreads))
    }
    cleaners
  }

  val tmpDiskInfos = JavaUtils.newConcurrentHashMap[String, DiskInfo]()
  disksSnapshot().foreach { diskInfo =>
    tmpDiskInfos.put(diskInfo.mountPoint, diskInfo)
  }
  private val deviceMonitor =
    DeviceMonitor.createDeviceMonitor(conf, this, deviceInfos, tmpDiskInfos, workerSource)

  private val byteBufAllocator: PooledByteBufAllocator =
    NettyUtils.getPooledByteBufAllocator(new TransportConf("StorageManager", conf), null, true)
  // (mountPoint -> LocalFlusher)
  private val (
    localFlushers: ConcurrentHashMap[String, LocalFlusher],
    _totalLocalFlusherThread: Int) = {
    val flushers = JavaUtils.newConcurrentHashMap[String, LocalFlusher]()
    var totalThread = 0;
    disksSnapshot().foreach { diskInfo =>
      if (!flushers.containsKey(diskInfo.mountPoint)) {
        val flusher = new LocalFlusher(
          workerSource,
          deviceMonitor,
          diskInfo.threadCount,
          byteBufAllocator,
          conf.workerPushMaxComponents,
          diskInfo.mountPoint,
          diskInfo.storageType,
          diskInfo.flushTimeMetrics)
        flushers.put(diskInfo.mountPoint, flusher)
        totalThread = totalThread + diskInfo.threadCount
      }
    }
    (flushers, totalThread)
  }

  deviceMonitor.startCheck()

  val hdfsDir = conf.hdfsDir
  val hdfsPermission = new FsPermission("755")
  val hdfsWriters = JavaUtils.newConcurrentHashMap[String, PartitionDataWriter]()
  val (hdfsFlusher, _totalHdfsFlusherThread) =
    if (hasHDFSStorage) {
      logInfo(s"Initialize HDFS support with path ${hdfsDir}")
      try {
        StorageManager.hadoopFs = CelebornHadoopUtils.getHadoopFS(conf)
      } catch {
        case e: Exception =>
          logError("Celeborn initialize HDFS failed.", e)
          throw e
      }
      (
        Some(new HdfsFlusher(
          workerSource,
          conf.workerHdfsFlusherThreads,
          byteBufAllocator,
          conf.workerPushMaxComponents)),
        conf.workerHdfsFlusherThreads)
    } else {
      (None, 0)
    }

  def totalFlusherThread: Int = _totalLocalFlusherThread + _totalHdfsFlusherThread

  override def notifyError(mountPoint: String, diskStatus: DiskStatus): Unit = this.synchronized {
    if (diskStatus == DiskStatus.CRITICAL_ERROR) {
      logInfo(s"Disk $mountPoint faces critical error, will remove its disk operator.")
      val operator = diskOperators.remove(mountPoint)
      if (operator != null) {
        operator.shutdown()
      }
    }
  }

  override def notifyHealthy(mountPoint: String): Unit = this.synchronized {
    if (!diskOperators.containsKey(mountPoint)) {
      diskOperators.put(
        mountPoint,
        ThreadUtils.newDaemonCachedThreadPool(
          s"disk-cleaner-$mountPoint",
          conf.workerDiskCleanThreads))
    }
  }

  private val counter = new AtomicInteger()
  private val counterOperator = new IntUnaryOperator() {
    override def applyAsInt(operand: Int): Int = {
      val dirs = healthyWorkingDirs()
      if (dirs.nonEmpty) {
        (operand + 1) % dirs.length
      } else 0
    }
  }

  // shuffleKey -> (fileName -> file info)
  private val diskFileInfos =
    JavaUtils.newConcurrentHashMap[String, ConcurrentHashMap[String, DiskFileInfo]]()
  private val RECOVERY_FILE_NAME_PREFIX = "recovery"
  private var RECOVERY_FILE_NAME = "recovery.ldb"
  private var db: DB = null
  private var saveCommittedFileInfosExecutor: ScheduledExecutorService = _
  private val saveCommittedFileInfoBySyncMethod =
    conf.workerGracefulShutdownSaveCommittedFileInfoSync
  private val saveCommittedFileInfoInterval =
    conf.workerGracefulShutdownSaveCommittedFileInfoInterval
  private var committedFileInfos: ConcurrentHashMap[String, ConcurrentHashMap[String, FileInfo]] = _
  // ShuffleClient can fetch data from a restarted worker only
  // when the worker's fetching port is stable.
  val workerGracefulShutdown = conf.workerGracefulShutdown
  if (workerGracefulShutdown) {
    try {
      val dbBackend = DBBackend.byName(conf.workerGracefulShutdownRecoverDbBackend)
      RECOVERY_FILE_NAME = dbBackend.fileName(RECOVERY_FILE_NAME_PREFIX)
      val recoverFile = new File(conf.workerGracefulShutdownRecoverPath, RECOVERY_FILE_NAME)
      this.db = DBProvider.initDB(dbBackend, recoverFile, CURRENT_VERSION)
      reloadAndCleanFileInfos(this.db)
    } catch {
      case e: Exception =>
        logError("Init level DB failed:", e)
        this.db = null
    }
    committedFileInfos =
      JavaUtils.newConcurrentHashMap[String, ConcurrentHashMap[String, FileInfo]]()
    saveCommittedFileInfosExecutor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "StorageManager-save-committed-fileinfo-thread")
    saveCommittedFileInfosExecutor.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          if (!committedFileInfos.isEmpty) {
            logInfo(s"Save committed fileinfo with ${committedFileInfos.size()} shuffle keys")
            committedFileInfos.asScala.foreach { case (shuffleKey, files) =>
              db.put(
                dbShuffleKey(shuffleKey),
                PbSerDeUtils.toPbFileInfoMap(files),
                saveCommittedFileInfoBySyncMethod)
            }
          }
        }
      },
      saveCommittedFileInfoInterval,
      saveCommittedFileInfoInterval,
      TimeUnit.MILLISECONDS)
  }
  cleanupExpiredAppDirs(System.currentTimeMillis() - storageExpireDirTimeout)
  if (!checkIfWorkingDirCleaned) {
    logWarning(
      "Worker still has residual files in the working directory before registering with Master, " +
        "please refer to the configuration document to increase " +
        s"${CelebornConf.WORKER_CHECK_FILE_CLEAN_TIMEOUT.key}.")
  } else {
    logInfo("Successfully remove all files under working directory.")
  }

  private def reloadAndCleanFileInfos(db: DB): Unit = {
    if (db != null) {
      val cache = JavaUtils.newConcurrentHashMap[String, UserIdentifier]()
      val itr = db.iterator
      itr.seek(SHUFFLE_KEY_PREFIX.getBytes(StandardCharsets.UTF_8))
      while (itr.hasNext) {
        val entry = itr.next
        val key = new String(entry.getKey, StandardCharsets.UTF_8)
        if (key.startsWith(SHUFFLE_KEY_PREFIX)) {
          val shuffleKey = parseDbShuffleKey(key)
          try {
            val files = PbSerDeUtils.fromPbFileInfoMap(entry.getValue, cache)
            logDebug(s"Reload DB: $shuffleKey -> $files")
            diskFileInfos.put(shuffleKey, files)
            db.delete(entry.getKey)
          } catch {
            case exception: Exception =>
              logError(s"Reload DB: $shuffleKey failed.", exception)
          }
        } else {
          return
        }
      }
    }
  }

  def saveAllCommittedFileInfosToDB(): Unit = {
    // save committed fileinfo to DB should be done within the time of saveCommittedFileInfoInterval
    saveCommittedFileInfosExecutor.awaitTermination(saveCommittedFileInfoInterval, MILLISECONDS)
    // graceful shutdown might be timed out, persist all committed fileinfos to DB
    // final flush write through
    committedFileInfos.asScala.foreach { case (shuffleKey, files) =>
      try {
        // K8s container might gone
        db.put(
          dbShuffleKey(shuffleKey),
          PbSerDeUtils.toPbFileInfoMap(files),
          true)
        logDebug(s"Update FileInfos into DB: $shuffleKey -> $files")
      } catch {
        case exception: Exception =>
          logError(s"Update FileInfos into DB: ${shuffleKey} failed.", exception)
      }
    }
  }

  private def getNextIndex() = counter.getAndUpdate(counterOperator)

  private val newMapFunc =
    new java.util.function.Function[String, ConcurrentHashMap[String, FileInfo]]() {
      override def apply(key: String): ConcurrentHashMap[String, FileInfo] =
        JavaUtils.newConcurrentHashMap()
    }

  private val diskFileInfoMapFunc =
    new java.util.function.Function[String, ConcurrentHashMap[String, DiskFileInfo]]() {
      override def apply(key: String): ConcurrentHashMap[String, DiskFileInfo] =
        JavaUtils.newConcurrentHashMap()
    }

  private val workingDirWriterListFunc =
    new java.util.function.Function[File, ConcurrentHashMap[String, PartitionDataWriter]]() {
      override def apply(t: File): ConcurrentHashMap[String, PartitionDataWriter] =
        JavaUtils.newConcurrentHashMap[String, PartitionDataWriter]()
    }

  @throws[IOException]
  def createPartitionDataWriter(
      appId: String,
      shuffleId: Int,
      location: PartitionLocation,
      splitThreshold: Long,
      splitMode: PartitionSplitMode,
      partitionType: PartitionType,
      rangeReadFilter: Boolean,
      userIdentifier: UserIdentifier): PartitionDataWriter = {
    createPartitionDataWriter(
      appId,
      shuffleId,
      location,
      splitThreshold,
      splitMode,
      partitionType,
      rangeReadFilter,
      userIdentifier,
      true)
  }

  @throws[IOException]
  def createPartitionDataWriter(
      appId: String,
      shuffleId: Int,
      location: PartitionLocation,
      splitThreshold: Long,
      splitMode: PartitionSplitMode,
      partitionType: PartitionType,
      rangeReadFilter: Boolean,
      userIdentifier: UserIdentifier,
      partitionSplitEnabled: Boolean): PartitionDataWriter = {
    if (healthyWorkingDirs().size <= 0 && !hasHDFSStorage) {
      throw new IOException("No available working dirs!")
    }
    val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
    val (flusher, diskFileInfo, workingDir) = createFile(
      location,
      appId,
      shuffleId,
      location.getFileName,
      userIdentifier,
      partitionType,
      partitionSplitEnabled)
    val writer =
      try {
        partitionType match {
          case PartitionType.MAP => new MapPartitionDataWriter(
              this,
              diskFileInfo,
              flusher,
              workerSource,
              conf,
              deviceMonitor,
              splitThreshold,
              splitMode,
              rangeReadFilter,
              shuffleKey)
          case PartitionType.REDUCE => new ReducePartitionDataWriter(
              this,
              diskFileInfo,
              flusher,
              workerSource,
              conf,
              deviceMonitor,
              splitThreshold,
              splitMode,
              rangeReadFilter,
              shuffleKey)
          case _ => throw new UnsupportedOperationException(s"Not support $partitionType yet")
        }
      } catch {
        case e: Exception =>
          logError("Create partition data writer failed", e)
          throw e
      }
    if (!(writer.getDiskFileInfo.isHdfs)) {
      deviceMonitor.registerFileWriter(writer)
      workingDirWriters.computeIfAbsent(workingDir, workingDirWriterListFunc).put(
        diskFileInfo.getFilePath,
        writer)
    }
    writer
  }

  def getDiskFileInfo(shuffleKey: String, fileName: String): DiskFileInfo = {
    val shuffleMap = diskFileInfos.get(shuffleKey)
    if (shuffleMap ne null) {
      shuffleMap.get(fileName)
    } else {
      null
    }
  }

  def getFetchTimeMetric(file: File): TimeWindow = {
    if (diskInfos != null) {
      val diskInfo = diskInfos.get(DeviceInfo.getMountPoint(file.getAbsolutePath, diskInfos))
      if (diskInfo != null) {
        diskInfo.fetchTimeMetrics
      } else null
    } else null
  }

  def shuffleKeySet(): util.HashSet[String] = {
    val hashSet = new util.HashSet[String]()
    hashSet.addAll(diskFileInfos.keySet())
    hashSet
  }

  def topAppDiskUsage: util.Map[String, Long] = {
    diskFileInfos.asScala.map { keyedWriters =>
      {
        keyedWriters._1 -> keyedWriters._2.values().asScala.map(_.getFileLength).sum
      }
    }.toList.map { case (shuffleKey, usage) =>
      shuffleKey.split("-")(0) -> usage
    }.groupBy(_._1).map { case (key, values) =>
      key -> values.map(_._2).sum
    }.toSeq.sortBy(_._2).reverse.take(conf.metricsAppTopDiskUsageCount * 2).toMap.asJava
  }

  def cleanFile(shuffleKey: String, fileName: String): Unit = {
    val fileInfo = getDiskFileInfo(shuffleKey, fileName)
    if (fileInfo != null) {
      cleanFileInternal(shuffleKey, fileInfo)
    }
  }

  def cleanFileInternal(shuffleKey: String, fileInfo: DiskFileInfo): Boolean = {
    var isHdfsExpired = false
    if (fileInfo.isHdfs) {
      isHdfsExpired = true
      val hdfsFileWriter = hdfsWriters.get(fileInfo.getFilePath)
      if (hdfsFileWriter != null) {
        hdfsFileWriter.destroy(new IOException(
          s"Destroy FileWriter $hdfsFileWriter caused by shuffle $shuffleKey expired."))
        hdfsWriters.remove(fileInfo.getFilePath)
      }
    } else {
      val workingDir =
        fileInfo.getFile.getParentFile.getParentFile.getParentFile
      val writers = workingDirWriters.get(workingDir)
      if (writers != null) {
        val fileWriter = writers.get(fileInfo.getFilePath)
        if (fileWriter != null) {
          fileWriter.destroy(new IOException(
            s"Destroy FileWriter $fileWriter caused by shuffle $shuffleKey expired."))
          writers.remove(fileInfo.getFilePath)
        }
      }
    }

    isHdfsExpired
  }

  def cleanupExpiredShuffleKey(
      expiredShuffleKeys: util.HashSet[String],
      cleanDB: Boolean = true): Unit = {
    expiredShuffleKeys.asScala.foreach { shuffleKey =>
      logInfo(s"Cleanup expired shuffle $shuffleKey.")
      if (diskFileInfos.containsKey(shuffleKey)) {
        val removedFileInfos = diskFileInfos.remove(shuffleKey)
        var isHdfsExpired = false
        if (removedFileInfos != null) {
          removedFileInfos.asScala.foreach {
            case (_, fileInfo) =>
              if (cleanFileInternal(shuffleKey, fileInfo)) {
                isHdfsExpired = true
              }
          }
        }
        val (appId, shuffleId) = Utils.splitShuffleKey(shuffleKey)
        disksSnapshot().filter(diskInfo =>
          diskInfo.status == DiskStatus.HEALTHY
            || diskInfo.status == DiskStatus.HIGH_DISK_USAGE).foreach { diskInfo =>
          diskInfo.dirs.foreach { dir =>
            val file = new File(dir, s"$appId/$shuffleId")
            deleteDirectory(file, diskOperators.get(diskInfo.mountPoint))
          }
        }
        if (isHdfsExpired) {
          try {
            StorageManager.hadoopFs.delete(
              new Path(new Path(hdfsDir, conf.workerWorkingDir), s"$appId/$shuffleId"),
              true)
          } catch {
            case e: Exception => logWarning("Clean expired HDFS shuffle failed.", e)
          }
        }
        if (workerGracefulShutdown) {
          committedFileInfos.remove(shuffleKey)
          if (cleanDB) {
            db.delete(dbShuffleKey(shuffleKey))
          }
        }
      }
    }
  }

  private val storageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("storage-scheduler")

  storageScheduler.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        try {
          // Clean up dirs which it's application is expired.
          cleanupExpiredAppDirs(System.currentTimeMillis() - storageExpireDirTimeout)
        } catch {
          case exception: Exception =>
            logWarning(s"Cleanup expired shuffle data exception: ${exception.getMessage}")
        }
      }
    },
    0,
    30,
    TimeUnit.MINUTES)

  private def cleanupExpiredAppDirs(expireDuration: Long): Unit = {
    val diskInfoAndAppDirs = disksSnapshot()
      .filter(diskInfo =>
        diskInfo.status == DiskStatus.HEALTHY
          || diskInfo.status == DiskStatus.HIGH_DISK_USAGE)
      .map(diskInfo =>
        (diskInfo, diskInfo.dirs.filter(_.exists).flatMap(_.listFiles())))
    val appIds = shuffleKeySet().asScala.map(key => Utils.splitShuffleKey(key)._1)

    diskInfoAndAppDirs.foreach { case (diskInfo, appDirs) =>
      appDirs.foreach { appDir =>
        // Don't delete shuffleKey's data that exist correct shuffle file info.
        if (!appIds.contains(appDir.getName) && appDir.lastModified() < expireDuration) {
          val threadPool = diskOperators.get(diskInfo.mountPoint)
          deleteDirectory(appDir, threadPool)
          logInfo(s"Delete expired app dir $appDir.")
        }
      }
    }
  }

  private def deleteDirectory(dir: File, threadPool: ThreadPoolExecutor): Unit = {
    if (dir.exists()) {
      threadPool.submit(new Runnable {
        override def run(): Unit = {
          deleteDirectoryWithRetry(dir)
        }
      })
    }
  }

  private def deleteDirectoryWithRetry(dir: File): Unit = {
    var retryCount = 0
    var deleteSuccess = false
    while (!deleteSuccess && retryCount <= 3) {
      try {
        FileUtils.deleteDirectory(dir)
        deleteSuccess = true
      } catch {
        case _: IOException =>
          retryCount = retryCount + 1
      }
    }
  }

  private def checkIfWorkingDirCleaned: Boolean = {
    var retryTimes = 0
    val workerCheckFileCleanTimeout = conf.workerCheckFileCleanTimeout
    val appIds = shuffleKeySet().asScala.map(key => Utils.splitShuffleKey(key)._1)
    while (retryTimes < conf.workerCheckFileCleanMaxRetries) {
      val localCleaned =
        !disksSnapshot().filter(_.status != DiskStatus.IO_HANG).exists { diskInfo =>
          diskInfo.dirs.exists {
            case workingDir if workingDir.exists() =>
              // Don't check appDirs that store information in the fileInfos
              workingDir.listFiles().exists(appDir => !appIds.contains(appDir.getName))
            case _ =>
              false
          }
        }

      val hdfsCleaned = hadoopFs match {
        case hdfs: FileSystem =>
          val hdfsWorkPath = new Path(hdfsDir, conf.workerWorkingDir)
          // HDFS path not exist when first time initialize
          if (hdfs.exists(hdfsWorkPath)) {
            !hdfs.listFiles(hdfsWorkPath, false).hasNext
          } else {
            true
          }
        case _ =>
          true
      }

      if (localCleaned && hdfsCleaned) {
        return true
      }
      retryTimes += 1
      if (retryTimes < conf.workerCheckFileCleanMaxRetries) {
        logInfo(s"Working directory's files have not been cleaned up completely, " +
          s"will start ${retryTimes + 1}th attempt after $workerCheckFileCleanTimeout milliseconds.")
      }
      Thread.sleep(workerCheckFileCleanTimeout)
    }
    false
  }

  def close(exitKind: Int): Unit = {
    if (db != null) {
      if (exitKind == CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN) {
        try {
          saveAllCommittedFileInfosToDB()
          db.close()
        } catch {
          case exception: Exception =>
            logError("Store recover data to DB failed.", exception)
        }
      } else {
        if (db != null) {
          db.close()
          new File(conf.workerGracefulShutdownRecoverPath, RECOVERY_FILE_NAME).delete()
        }
      }
    }
    if (null != diskOperators) {
      if (exitKind != CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN) {
        cleanupExpiredShuffleKey(shuffleKeySet(), false)
      }
      ThreadUtils.parmap(
        diskOperators.asScala.toMap,
        "ShutdownDiskOperators",
        diskOperators.size()) { entry =>
        ThreadUtils.shutdown(
          entry._2,
          conf.workerGracefulShutdownFlusherShutdownTimeoutMs.milliseconds)
      }
    }
    storageScheduler.shutdownNow()
    if (null != deviceMonitor) {
      deviceMonitor.close()
    }
  }

  private def flushFileWriters(): Unit = {
    workingDirWriters.forEach(new BiConsumer[File, ConcurrentHashMap[String, PartitionDataWriter]] {
      override def accept(
          t: File,
          writers: ConcurrentHashMap[String, PartitionDataWriter]): Unit = {
        writers.forEach(new BiConsumer[String, PartitionDataWriter] {
          override def accept(file: String, writer: PartitionDataWriter): Unit = {
            if (writer.getException == null) {
              try {
                writer.flushOnMemoryPressure()
              } catch {
                case t: Throwable =>
                  logError(
                    s"FileWrite of $writer faces unexpected exception when flush on memory pressure.",
                    t)
              }
            } else {
              logWarning(s"Skip flushOnMemoryPressure because ${writer.flusher} " +
                s"has error: ${writer.getException.getMessage}")
            }
          }
        })
      }
    })
    hdfsWriters.forEach(new BiConsumer[String, PartitionDataWriter] {
      override def accept(t: String, u: PartitionDataWriter): Unit = {
        u.flushOnMemoryPressure()
      }
    })
  }

  override def onPause(moduleName: String): Unit = {}

  override def onResume(moduleName: String): Unit = {}

  override def onTrim(): Unit = {
    logInfo(s"Trigger ${this.getClass.getCanonicalName} trim action")
    try {
      flushFileWriters()
      Thread.sleep(conf.workerDirectMemoryTrimFlushWaitInterval)
    } catch {
      case e: Exception =>
        logError(s"Trigger ${this.getClass.getCanonicalName} trim failed.", e)
    }
  }

  def updateDiskInfos(): Unit = this.synchronized {
    disksSnapshot().filter(_.status != DiskStatus.IO_HANG).foreach { diskInfo =>
      val totalUsage = diskInfo.dirs.map { dir =>
        val writers = workingDirWriters.get(dir)
        if (writers != null) {
          writers.synchronized {
            writers.values.asScala.map(_.getDiskFileInfo.getFileLength).sum
          }
        } else {
          0
        }
      }.sum
      val fileSystemReportedUsableSpace = Files.getFileStore(
        Paths.get(diskInfo.mountPoint)).getUsableSpace
      val workingDirUsableSpace =
        Math.min(diskInfo.configuredUsableSpace - totalUsage, fileSystemReportedUsableSpace)
      logDebug(s"updateDiskInfos  workingDirUsableSpace:$workingDirUsableSpace filemeta:$fileSystemReportedUsableSpace conf:${diskInfo.configuredUsableSpace} totalUsage:$totalUsage")
      diskInfo.setUsableSpace(workingDirUsableSpace)
      diskInfo.updateFlushTime()
      diskInfo.updateFetchTime()
    }
    logInfo(s"Updated diskInfos:\n${disksSnapshot().mkString("\n")}")
  }

  def userResourceConsumptionSnapshot(): Map[UserIdentifier, ResourceConsumption] = {
    diskFileInfos.synchronized {
      // shuffleId -> (fileName -> fileInfo)
      diskFileInfos
        .asScala
        .toList
        .flatMap { case (_, fileInfoMaps) =>
          // userIdentifier -> fileInfo
          fileInfoMaps.values().asScala.map { fileInfo =>
            (fileInfo.getUserIdentifier, fileInfo)
          }
        }
        // userIdentifier -> List((userIdentifier, fileInfo))
        .groupBy(_._1)
        .map { case (userIdentifier, userWithFileInfoList) =>
          // collect resource consumed by each user on this worker
          val resourceConsumption = {
            val userFileInfos = userWithFileInfoList.map(_._2)
            val diskFileInfos = userFileInfos.filter(!_.isHdfs)
            val hdfsFileInfos = userFileInfos.filter(_.isHdfs)

            val diskBytesWritten = diskFileInfos.map(_.getFileLength).sum
            val diskFileCount = diskFileInfos.size
            val hdfsBytesWritten = hdfsFileInfos.map(_.getFileLength).sum
            val hdfsFileCount = hdfsFileInfos.size
            ResourceConsumption(diskBytesWritten, diskFileCount, hdfsBytesWritten, hdfsFileCount)
          }
          (userIdentifier, resourceConsumption)
        }
    }
  }

  def notifyFileInfoCommitted(
      shuffleKey: String,
      fileName: String,
      fileInfo: FileInfo): Unit = {
    committedFileInfos.computeIfAbsent(shuffleKey, newMapFunc).put(fileName, fileInfo)
  }

  def getActiveShuffleSize(): Long = {
    diskFileInfos.values().asScala.map(_.values().asScala.map(_.getBytesFlushed).sum).sum
  }

  def getActiveShuffleFileCount(): Long = {
    diskFileInfos.asScala.values.map(_.size()).sum
  }

  def createFile(
      location: PartitionLocation,
      appId: String,
      shuffleId: Int,
      fileName: String,
      userIdentifier: UserIdentifier,
      partitionType: PartitionType,
      partitionSplitEnabled: Boolean): (Flusher, DiskFileInfo, File) = {
    val suggestedMountPoint = location.getStorageInfo.getMountPoint
    var retryCount = 0
    var exception: IOException = null
    val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
    while (retryCount < conf.workerCreateWriterMaxAttempts) {
      val diskInfo = diskInfos.get(suggestedMountPoint)
      val dirs =
        if (diskInfo != null && diskInfo.status.equals(DiskStatus.HEALTHY)) {
          diskInfo.dirs
        } else {
          logDebug(s"Disk unavailable for $suggestedMountPoint, return all healthy" +
            s" working dirs. diskInfo $diskInfo")
          healthyWorkingDirs()
        }
      if (dirs.isEmpty && hdfsFlusher.isEmpty) {
        throw new IOException(s"No available disks! suggested mountPoint $suggestedMountPoint")
      }

      if (dirs.isEmpty && location.getStorageInfo.HDFSAvailable()) {
        val shuffleDir =
          new Path(new Path(hdfsDir, conf.workerWorkingDir), s"$appId/$shuffleId")
        FileSystem.mkdirs(StorageManager.hadoopFs, shuffleDir, hdfsPermission)
        val hdfsFilePath = new Path(shuffleDir, fileName).toString
        val hdfsFileInfo = new DiskFileInfo(
          userIdentifier,
          partitionSplitEnabled,
          new ReduceFileMeta(),
          hdfsFilePath,
          StorageInfo.Type.HDFS)
        diskFileInfos.computeIfAbsent(shuffleKey, diskFileInfoMapFunc).put(
          fileName,
          hdfsFileInfo)
        return (hdfsFlusher.get, hdfsFileInfo, null)
      } else if (dirs.nonEmpty && location.getStorageInfo.localDiskAvailable()) {
        val dir = dirs(getNextIndex() % dirs.size)
        val mountPoint = DeviceInfo.getMountPoint(dir.getAbsolutePath, mountPoints)
        val shuffleDir = new File(dir, s"$appId/$shuffleId")
        shuffleDir.mkdirs()
        val file = new File(shuffleDir, fileName)
        try {
          if (file.exists()) {
            throw new FileAlreadyExistsException(
              s"Shuffle data file ${file.getAbsolutePath} already exists.")
          } else {
            val createFileSuccess = file.createNewFile()
            if (!createFileSuccess) {
              throw new CelebornException(
                s"Create shuffle data file ${file.getAbsolutePath} failed!")
            }
          }
          val filePath = file.getAbsolutePath
          val fileMeta = partitionType match {
            case PartitionType.REDUCE =>
              new ReduceFileMeta()
            case PartitionType.MAP =>
              new MapFileMeta()
            case PartitionType.MAPGROUP =>
              throw new NotImplementedError("Map group is not implemented")
          }
          val diskFileInfo = new DiskFileInfo(
            userIdentifier,
            partitionSplitEnabled,
            fileMeta,
            filePath,
            StorageInfo.Type.HDD)
          diskFileInfos.computeIfAbsent(shuffleKey, diskFileInfoMapFunc).put(
            fileName,
            diskFileInfo)
          return (
            localFlushers.get(mountPoint),
            diskFileInfo,
            dir)
        } catch {
          case fe: FileAlreadyExistsException =>
            logError("Failed to create fileWriter because of existed file", fe)
            throw fe
          case t: Throwable =>
            logError(
              s"Create FileWriter for ${file.getAbsolutePath} of mount $mountPoint " +
                s"failed, report to DeviceMonitor",
              t)
            deviceMonitor.reportNonCriticalError(
              mountPoint,
              new IOException(t),
              DiskStatus.READ_OR_WRITE_FAILURE)
            throw t
        }
      } else {
        exception = new IOException("No storage available for location:" + location.toString)
      }
      retryCount += 1
    }
    throw exception
  }
}

object StorageManager {
  var hadoopFs: FileSystem = _
}
