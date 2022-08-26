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

package com.aliyun.emr.rss.service.deploy.worker.storage

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.{ConcurrentHashMap, Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.function.IntUnaryOperator

import scala.collection.JavaConverters._

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.iq80.leveldb.DB

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.exception.RssException
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{DeviceInfo, DiskInfo, DiskStatus, FileInfo}
import com.aliyun.emr.rss.common.metrics.source.AbstractSource
import com.aliyun.emr.rss.common.network.server.MemoryTracker.MemoryTrackerListener
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType}
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.rss.common.utils.PBSerDeUtils
import com.aliyun.emr.rss.service.deploy.worker._
import com.aliyun.emr.rss.service.deploy.worker.storage.StorageManager.hdfsFs

private[worker] final class StorageManager(conf: RssConf, workerSource: AbstractSource)
  extends ShuffleRecoverHelper with DeviceObserver with Logging with MemoryTrackerListener{
  // mount point -> filewriter
  val workingDirWriters = new ConcurrentHashMap[File, util.ArrayList[FileWriter]]()

  val (deviceInfos, diskInfos) = {
    val workingDirInfos =
      RssConf.workerBaseDirs(conf).map { case (workdir, maxSpace, flusherThread, storageType) =>
        (new File(workdir, RssConf.workingDirName(conf)), maxSpace, flusherThread, storageType)
      }

    if (workingDirInfos.size <= 0) {
      throw new IOException("Empty working directory configuration!")
    }

    DeviceInfo.getDeviceAndDiskInfos(workingDirInfos)
  }
  val mountPoints = new util.HashSet[String](diskInfos.keySet())

  def disksSnapshot(): List[DiskInfo] = {
    diskInfos.synchronized {
      val disks = new util.ArrayList[DiskInfo](diskInfos.values())
      disks.asScala.toList
    }
  }

  def healthyWorkingDirs(): List[File] =
    disksSnapshot().filter(_.status == DiskStatus.Healthy).flatMap(_.dirs)

  private val diskOperators: ConcurrentHashMap[String, ThreadPoolExecutor] = {
    val cleaners = new ConcurrentHashMap[String, ThreadPoolExecutor]()
    disksSnapshot().foreach {
      diskInfo => cleaners.put(diskInfo.mountPoint,
        ThreadUtils.newDaemonCachedThreadPool(s"Disk-cleaner-${diskInfo.mountPoint}", 1))
    }
    cleaners
  }

  val tmpDiskInfos = new ConcurrentHashMap[String, DiskInfo]()
  disksSnapshot().foreach{ case diskInfo =>
    tmpDiskInfos.put(diskInfo.mountPoint, diskInfo)
  }
  private val deviceMonitor =
    DeviceMonitor.createDeviceMonitor(conf, this, deviceInfos, tmpDiskInfos)

  // (mountPoint -> LocalFlusher)
  private val localFlushers: ConcurrentHashMap[String, LocalFlusher] = {
    val flushers = new ConcurrentHashMap[String, LocalFlusher]()
    disksSnapshot().foreach { case diskInfo =>
      if (!flushers.containsKey(diskInfo.mountPoint)) {
        val flusher = new LocalFlusher(
          workerSource,
          deviceMonitor,
          diskInfo.threadCount,
          diskInfo.mountPoint,
          RssConf.flushAvgTimeWindow(conf),
          RssConf.flushAvgTimeMinimumCount(conf),
          diskInfo.storageType
        )
        flushers.put(diskInfo.mountPoint, flusher)
      }
    }
    flushers
  }

  private val actionService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
    .setNameFormat("StorageManager-action-thread").build)

  deviceMonitor.startCheck()

  val hdfsDir = RssConf.hdfsDir(conf)
  val hdfsPermission = FsPermission.createImmutable(755)
  val hdfsWriters = new util.ArrayList[FileWriter]()
  val hdfsFlusher = if (!hdfsDir.isEmpty) {
    val hdfsConfiguration = new Configuration
    hdfsConfiguration.set("fs.defaultFS", hdfsDir)
    hdfsConfiguration.set("dfs.replication", "2")
    StorageManager.hdfsFs = FileSystem.get(hdfsConfiguration)
    Some(new HdfsFlusher(
      workerSource,
      RssConf.hdfsFlusherThreadCount(conf),
      RssConf.flushAvgTimeWindow(conf),
      RssConf.flushAvgTimeMinimumCount(conf)))
  } else {
    None
  }

  lazy val heartBeatCount = new AtomicLong()
  lazy val hdfsDelayedCleanMap = new ConcurrentHashMap[String, Long]()
  lazy val hdfsCheckCleanPool =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("Hdfs-Cleaner-Scheduler")
  lazy val hdfsCleanerDelay = RssConf.hdfsCleanDelayHeartBeatCount(conf)
  lazy val hdfsCleanActionPool = ThreadUtils.newDaemonCachedThreadPool("Hdfs-Cleaner", 8, 60)
  hdfsCheckCleanPool.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      val currentHeartBeatCount = heartBeatCount.get()
      val filesToDelete = hdfsDelayedCleanMap.asScala
        .filter(p => (currentHeartBeatCount - p._2 > hdfsCleanerDelay))
        .map(_._1)
      if (filesToDelete.nonEmpty) {
        for (file <- filesToDelete) {
          hdfsDelayedCleanMap.remove(file)
          hdfsCleanActionPool.submit(new Runnable {
            override def run(): Unit = {
              try {
                StorageManager.hdfsFs.delete(new Path(file), true)
              } catch {
                case ioe: IOException =>
                  log.warn(s"Clean hdfs file ${file} failed", ioe)
              }
            }
          })
        }
      }
    }
  }, RssConf.workerTimeoutMs(conf) / 8, RssConf.workerTimeoutMs(conf) / 8, TimeUnit.MILLISECONDS)

  override def notifyError(mountPoint: String, diskStatus: DiskStatus): Unit = this.synchronized {
    if (diskStatus == DiskStatus.IoHang) {
      logInfo("IoHang, remove disk operator")
      val operator = diskOperators.remove(mountPoint)
      if (operator != null) {
        operator.shutdown()
      }
    }
  }

  override def notifyHealthy(mountPoint: String): Unit = this.synchronized {
    if (!diskOperators.containsKey(mountPoint)) {
      diskOperators.put(mountPoint,
        ThreadUtils.newDaemonCachedThreadPool(s"Disk-cleaner-${mountPoint}", 1))
    }
  }

  private val counter = new AtomicInteger()
  private val counterOperator = new IntUnaryOperator() {
    override def applyAsInt(operand: Int): Int = {
      val dirs = healthyWorkingDirs()
      if (dirs.length > 0) {
        (operand + 1) % dirs.length
      } else 0
    }
  }

  // shuffleKey -> (fileName -> file info)
  private val fileInfos =
    new ConcurrentHashMap[String, ConcurrentHashMap[String, FileInfo]]()
  private val RECOVERY_FILE_INFOS_FILE_NAME = "fileInfos.ldb"
  private var fileInfosDb: DB = null
  // ShuffleClient can fetch data from a restarted worker only
  // when the worker's fetching port is stable.
  if (RssConf.workerGracefulShutdown(conf)) {
    try {
      val recoverFile = new File(RssConf.workerRecoverPath(conf), RECOVERY_FILE_INFOS_FILE_NAME)
      this.fileInfosDb = LevelDBProvider.initLevelDB(recoverFile, CURRENT_VERSION)
      reloadAndCleanFileInfos(this.fileInfosDb)
    } catch {
      case e: Exception =>
        logError("Init level DB failed:", e)
        this.fileInfosDb = null
    }
  }

  private def reloadAndCleanFileInfos(db: DB): Unit = {
    if (db != null) {
      val itr = db.iterator
      itr.seek(SHUFFLE_KEY_PREFIX.getBytes(StandardCharsets.UTF_8))
      while (itr.hasNext) {
        val entry = itr.next
        val key = new String(entry.getKey, StandardCharsets.UTF_8)
        if (key.startsWith(SHUFFLE_KEY_PREFIX)) {
          val shuffleKey = parseDbShuffleKey(key)
          try {
            val files = PBSerDeUtils.fromPbFileInfoMap(entry.getValue)
            logDebug("Reload DB: " + shuffleKey + " -> " + files)
            fileInfos.put(shuffleKey, files)
            fileInfosDb.delete(entry.getKey)
          } catch {
            case exception: Exception =>
              logError("Reload DB: " + shuffleKey + " failed.", exception);
          }
        }
      }
    }
  }

  def updateFileInfosInDB(): Unit = {
    fileInfos.asScala.foreach { case (shuffleKey, files) =>
      try {
        fileInfosDb.put(dbShuffleKey(shuffleKey), PBSerDeUtils.toPbFileInfoMap(files))
        logDebug("Update DB: " + shuffleKey + " -> " + files)
      } catch {
        case exception: Exception =>
         logError("Update DB: " + shuffleKey + " failed.", exception)
      }
    }
  }

  private def getNextIndex() = counter.getAndUpdate(counterOperator)

  private val newMapFunc =
    new java.util.function.Function[String, ConcurrentHashMap[String, FileInfo]]() {
      override def apply(key: String): ConcurrentHashMap[String, FileInfo] =
        new ConcurrentHashMap()
    }

  private val workingDirWriterListFunc =
    new java.util.function.Function[File, util.ArrayList[FileWriter]]() {
      override def apply(t: File): util.ArrayList[FileWriter] = new util.ArrayList[FileWriter]()
    }

  @throws[IOException]
  def createWriter(
      appId: String,
      shuffleId: Int,
      location: PartitionLocation,
      splitThreshold: Long,
      splitMode: PartitionSplitMode,
      partitionType: PartitionType): FileWriter = {
    if (healthyWorkingDirs().size <= 0) {
      throw new IOException("No available working dirs!")
    }

    val fileName = location.getFileName
    val hasReplication = location.getPeer != null
    var retryCount = 0
    var exception: IOException = null
    val suggestedMountPoint = location.getStorageHint.getMountPoint
    while (retryCount < RssConf.createFileWriterRetryCount(conf)) {
      val diskInfo = diskInfos.get(suggestedMountPoint)
      val dirs = if (diskInfo != null && diskInfo.status.equals(DiskStatus.Healthy)) {
        diskInfo.dirs
      } else {
        logWarning(s"Disk unavailable for $suggestedMountPoint, return all healthy" +
          s" working dirs. diskInfo $diskInfo")
        healthyWorkingDirs()
      }
      if (dirs.isEmpty && hdfsFlusher.isEmpty) {
        throw new IOException(s"No available disks! suggested mountPoint $suggestedMountPoint")
      }
      val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
      if (dirs.isEmpty) {
        val shuffleDir = new Path(new Path(hdfsDir, RssConf.workingDirName(conf)),
          s"$appId/$shuffleId")
        FileSystem.mkdirs(StorageManager.hdfsFs, shuffleDir, hdfsPermission)
        val fileInfo = new FileInfo(new Path(shuffleDir, fileName).toString)
        val hdfsWriter = new FileWriter(
          fileInfo,
          hdfsFlusher.get,
          workerSource,
          conf,
          deviceMonitor,
          splitThreshold,
          splitMode,
          partitionType,
          hasReplication)
        fileInfos.computeIfAbsent(shuffleKey, newMapFunc).put(fileName, fileInfo)
        hdfsWriters.synchronized {
          hdfsWriters.add(hdfsWriter)
        }
        hdfsWriter.registerDestroyHook(hdfsWriters)
        return hdfsWriter
      } else {
        val dir = dirs(getNextIndex() % dirs.size)
        val mountPoint = DeviceInfo.getMountPoint(dir.getAbsolutePath, mountPoints)
        val shuffleDir = new File(dir, s"$appId/$shuffleId")
        val file = new File(shuffleDir, fileName)
        try {
          shuffleDir.mkdirs()
          val createFileSuccess = file.createNewFile()
          if (!createFileSuccess) {
            throw new RssException("create app shuffle data dir or file failed!" +
              s"${file.getAbsolutePath}")
          }
          val fileInfo = new FileInfo(file.getAbsolutePath)
          val fileWriter = new FileWriter(
            fileInfo,
            localFlushers.get(mountPoint),
            workerSource,
            conf,
            deviceMonitor,
            splitThreshold,
            splitMode,
            partitionType)
          deviceMonitor.registerFileWriter(fileWriter)
          val list = workingDirWriters.computeIfAbsent(dir, workingDirWriterListFunc)
          list.synchronized {
            list.add(fileWriter)
          }
          fileWriter.registerDestroyHook(list)
          fileInfos.computeIfAbsent(shuffleKey, newMapFunc).put(fileName, fileInfo)
          location.getStorageHint.setMountPoint(mountPoint)
          logDebug(s"location $location set disk hint to ${location.getStorageHint} ")
          return fileWriter
        } catch {
          case t: Throwable =>
            logError("Create Writer failed, report to DeviceMonitor", t)
            exception = new IOException(t)
            deviceMonitor.reportDeviceError(mountPoint, exception,
              DiskStatus.ReadOrWriteFailure)
        }
      }
      retryCount += 1
    }

    throw exception
  }

  def getFileInfo(shuffleKey: String, fileName: String): FileInfo = {
    val shuffleMap = fileInfos.get(shuffleKey)
    if (shuffleMap ne null) {
      shuffleMap.get(fileName)
    } else {
      null
    }
  }

  def shuffleKeySet(): util.Set[String] = fileInfos.keySet()

  def cleanupExpiredShuffleKey(expiredShuffleKeys: util.HashSet[String]): Unit = {
    expiredShuffleKeys.asScala.foreach { shuffleKey =>
      logInfo(s"Cleanup expired shuffle $shuffleKey.")
      val hasHdfs = fileInfos.remove(shuffleKey).asScala.filter(_._2.isHdfs).size > 0
      val (appId, shuffleId) = Utils.splitShuffleKey(shuffleKey)
      disksSnapshot().filter(_.status != DiskStatus.IoHang).foreach{ case diskInfo =>
        diskInfo.dirs.foreach { case dir =>
          val file = new File(dir, s"$appId/$shuffleId")
          deleteDirectory(file, diskOperators.get(diskInfo.mountPoint))
        }
      }
      if (hasHdfs) {
        val hdfsFilePath = new Path(new Path(hdfsDir, RssConf.workingDirName(conf)),
          s"$appId/$shuffleId").toString
        val hdfsFileSuccessPath = hdfsFilePath + FileWriter.SUFFIX_HDFS_WRITE_SUCCESS
        hdfsDelayedCleanMap.put(hdfsFilePath, heartBeatCount.get())
        hdfsDelayedCleanMap.put(hdfsFileSuccessPath, heartBeatCount.get())
      }
    }
  }

  private val noneEmptyDirExpireDurationMs = RssConf.appExpireDurationMs(conf)
  private val storageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("storage-scheduler")

  storageScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      try {
        // Clean up dirs which has not been modified
        // in the past {{noneEmptyExpireDurationsMs}}.
        cleanupExpiredAppDirs(System.currentTimeMillis() - noneEmptyDirExpireDurationMs)
      } catch {
        case exception: Exception =>
          logWarning(s"Cleanup expired shuffle data exception: ${exception.getMessage}")
      }
    }
  }, 0, 30, TimeUnit.MINUTES)

  private def cleanupExpiredAppDirs(expireTime: Long): Unit = {
    disksSnapshot().filter(_.status != DiskStatus.IoHang).foreach { case diskInfo =>
      diskInfo.dirs.foreach { case workingDir =>
        workingDir.listFiles().foreach{ case appDir =>
          if (appDir.lastModified() < expireTime) {
            val threadPool = diskOperators.get(diskInfo.mountPoint)
            deleteDirectory(appDir, threadPool)
            logInfo(s"Delete expired app dir $appDir.")
          }
        }
      }
    }

    if (hdfsFs != null) {
      val iter = hdfsFs.listFiles(new Path(hdfsDir, RssConf.workingDirName(conf)), false)
      while (iter.hasNext) {
        val fileStatus = iter.next()
        if (fileStatus.getModificationTime < expireTime) {
          hdfsDelayedCleanMap.put(fileStatus.getPath.toString, heartBeatCount.get())
        }
      }
    }
  }

  private def deleteDirectory(dir: File, threadPool: ThreadPoolExecutor): Unit = {
    val allContents = dir.listFiles
    if (allContents != null) {
      for (file <- allContents) {
        deleteDirectory(file, threadPool)
      }
    }
    threadPool.submit(new Runnable {
      override def run(): Unit = {
        deleteFileWithRetry(dir)
      }
    })
  }

  private def deleteFileWithRetry(file: File): Unit = {
    if (file.exists()) {
      var retryCount = 0
      var deleteSuccess = false
      while (!deleteSuccess && retryCount <= 3) {
        deleteSuccess = file.delete()
        retryCount = retryCount + 1
        if (!deleteSuccess) {
          Thread.sleep(200 * retryCount)
        }
      }
      if (deleteSuccess) {
        logDebug(s"Deleted expired shuffle file $file.")
      } else {
        logWarning(s"Failed to delete expired shuffle file $file.")
      }
    }
  }

  def close(): Unit = {
    if (fileInfosDb != null) {
      try {
        updateFileInfosInDB();
        fileInfosDb.close();
      } catch {
        case exception: Exception =>
          logError("Store recover data to LevelDB failed.", exception);
      }
    }
    if (null != diskOperators) {
      diskOperators.asScala.foreach(entry => {
        entry._2.shutdownNow()
      })
    }
    storageScheduler.shutdownNow()
    hdfsCheckCleanPool.shutdownNow()
    hdfsCleanActionPool.shutdownNow()
    if (null != deviceMonitor) {
      deviceMonitor.close()
    }
  }

  private def flushFileWriters(): Unit = {
    val allWriters = new util.HashSet[FileWriter]()
    workingDirWriters.asScala.foreach { case (_, writers) =>
      writers.synchronized {
        allWriters.addAll(writers)
      }
    }
    allWriters.asScala.foreach{ case writer =>
      writer.flushOnMemoryPressure()
    }
  }

  override def onPause(moduleName: String): Unit = {
  }

  override def onResume(moduleName: String): Unit = {
  }

  override def onTrim(): Unit = {
    actionService.submit(new Runnable {
      override def run(): Unit = {
        flushFileWriters()
      }
    })
  }

  def updateDiskInfos(): Unit = this.synchronized {
    disksSnapshot().filter(_.status != DiskStatus.IoHang).foreach { case diskInfo =>
      val totalUsage = diskInfo.dirs.map { dir =>
        val writers = workingDirWriters.get(dir)
        if (writers != null) {
          writers.synchronized {
            writers.asScala.map(_.getFileInfo.getFileLength).sum
          }
        } else {
          0
        }
      }.sum
      val fileSystemReportedUsableSpace = Files.getFileStore(
        Paths.get(diskInfo.mountPoint)).getUsableSpace
      val workingDirUsableSpace = Math.min(diskInfo.configuredUsableSpace - totalUsage,
        fileSystemReportedUsableSpace)
      val flushTimeAverage = localFlushers.get(diskInfo.mountPoint).averageFlushTime()
      diskInfo.setUsableSpace(workingDirUsableSpace)
      diskInfo.setFlushTime(flushTimeAverage)
    }
    logInfo(s"Updated diskInfos: ${disksSnapshot()}")
  }

  def onHeartBeat(): Unit = {
    heartBeatCount.incrementAndGet()
  }
}

object StorageManager {
  var hdfsFs: FileSystem = _
}
