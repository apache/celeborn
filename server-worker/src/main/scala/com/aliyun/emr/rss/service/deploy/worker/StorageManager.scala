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

package com.aliyun.emr.rss.service.deploy.worker

import java.io.{File, IOException}
import java.nio.channels.{ClosedByInterruptException, FileChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.{ConcurrentHashMap, Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLongArray, LongAdder}
import java.util.function.IntUnaryOperator

import scala.collection.JavaConverters._
import scala.util.Random

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.buffer.{ByteBufUtil, CompositeByteBuf, Unpooled}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.iq80.leveldb.DB

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.exception.RssException
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{DeviceInfo, DiskInfo}
import com.aliyun.emr.rss.common.meta.DiskStatus
import com.aliyun.emr.rss.common.metrics.source.AbstractSource
import com.aliyun.emr.rss.common.network.server.{FileInfo, MemoryTracker}
import com.aliyun.emr.rss.common.network.server.MemoryTracker.MemoryTrackerListener
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.rss.common.utils.PBSerDeUtils
import com.aliyun.emr.rss.service.deploy.worker.FileWriter.FlushNotifier

trait DeviceObserver {
  def notifyError(mountPoint: String, diskStatus: DiskStatus): Unit = {}
  def notifyHealthy(mountPoint: String): Unit = {}
  def notifyHighDiskUsage(mountPoint: String): Unit = {}
}

private[worker] abstract class FlushTask(
    val buffer: CompositeByteBuf,
    val notifier: FlushNotifier) {
  def flush(): Unit
}

private[worker] class LocalFlushTask(
    buffer: CompositeByteBuf,
    fileChannel: FileChannel,
    notifier: FileWriter.FlushNotifier) extends FlushTask(buffer, notifier) {
  override def flush(): Unit = {
    fileChannel.write(buffer.nioBuffers())
  }
}

private[worker] class HdfsFlushTask(
  buffer: CompositeByteBuf,
  fsStream: FSDataOutputStream,
  notifier: FileWriter.FlushNotifier) extends FlushTask(buffer, notifier) {
  override def flush(): Unit = {
    fsStream.write(ByteBufUtil.getBytes(buffer))
  }
}

private[worker] abstract class Flusher(
    val workerSource: AbstractSource,
    val threadCount: Int,
    val flushAvgTimeWindowSize: Int,
    val flushAvgTimeMinimumCount: Int) extends Logging {
  protected lazy val flusherId = System.identityHashCode(this)
  protected val workingQueues = new Array[LinkedBlockingQueue[FlushTask]](threadCount)
  protected val bufferQueue = new LinkedBlockingQueue[CompositeByteBuf]()
  protected val workers = new Array[Thread](threadCount)
  protected var nextWorkerIndex: Int = 0
  protected val flushCount = new LongAdder
  protected val flushTotalTime = new LongAdder
  protected val avgTimeWindow = new Array[(Long, Long)](flushAvgTimeWindowSize)
  protected var avgTimeWindowCurrentIndex = 0

  val lastBeginFlushTime: AtomicLongArray = new AtomicLongArray(threadCount)
  val stopFlag = new AtomicBoolean(false)
  val rand = new Random()

  init()

  private def init(): Unit = {
    for (i <- 0 until flushAvgTimeWindowSize) {
      avgTimeWindow(i) = (0L, 0L)
    }
    for (i <- 0 until lastBeginFlushTime.length()) {
      lastBeginFlushTime.set(i, -1)
    }
    for (index <- 0 until threadCount) {
      workingQueues(index) = new LinkedBlockingQueue[FlushTask]()
      workers(index) = new Thread(s"$this-$index") {
        override def run(): Unit = {
          while (!stopFlag.get()) {
            val task = workingQueues(index).take()
            val key = s"Flusher-$this-${rand.nextInt()}"
            workerSource.sample(WorkerSource.FlushDataTime, key) {
              if (!task.notifier.hasException) {
                try {
                  val flushBeginTime = System.nanoTime()
                  lastBeginFlushTime.set(index, flushBeginTime)
                  task.flush()
                  flushTotalTime.add(System.nanoTime() - flushBeginTime)
                  flushCount.increment()
                } catch {
                  case _: ClosedByInterruptException =>
                  case e: IOException =>
                    task.notifier.setException(e)
                    processIOException(e, DiskStatus.ReadOrWriteFailure)
                }
                lastBeginFlushTime.set(index, -1)
              }
              returnBuffer(task.buffer)
              task.notifier.numPendingFlushes.decrementAndGet()
            }
          }
        }
      }
      workers(index).setDaemon(true)
      workers(index).setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          logError(s"$this thread terminated.", e)
        }
      })
      workers(index).start()
    }
  }

  def getWorkerIndex: Int = synchronized {
    nextWorkerIndex = (nextWorkerIndex + 1) % threadCount
    nextWorkerIndex
  }

  def averageFlushTime(): Long = {
    if (this.isInstanceOf[LocalFlusher]) {
      logInfo(s"Flush count in ${this.asInstanceOf[LocalFlusher].mountPoint}" +
        s" last heartbeat interval: $flushCount")
    }
    val currentFlushTime = flushTotalTime.sumThenReset()
    val currentFlushCount = flushCount.sumThenReset()
    if (currentFlushCount >= flushAvgTimeMinimumCount) {
      avgTimeWindow(avgTimeWindowCurrentIndex) = (currentFlushTime, currentFlushCount)
      avgTimeWindowCurrentIndex = (avgTimeWindowCurrentIndex + 1) % flushAvgTimeWindowSize
    }

    var totalFlushTime = 0L
    var totalFlushCount = 0L
    avgTimeWindow.foreach { case (flushTime, flushCount) =>
      totalFlushTime = totalFlushTime + flushTime
      totalFlushCount = totalFlushCount + flushCount
    }

    if (totalFlushCount != 0) {
      totalFlushTime / totalFlushCount
    } else {
      0L
    }
  }

  def takeBuffer(): CompositeByteBuf = {
    var buffer = bufferQueue.poll()
    if (buffer == null) {
      buffer = Unpooled.compositeBuffer(256)
    }
    buffer
  }

  def returnBuffer(buffer: CompositeByteBuf): Unit = {
    MemoryTracker.instance().releaseDiskBuffer(buffer.readableBytes())
    buffer.removeComponents(0, buffer.numComponents())
    buffer.clear()

    bufferQueue.put(buffer)
  }

  def addTask(task: FlushTask, timeoutMs: Long, workerIndex: Int): Boolean = {
    workingQueues(workerIndex).offer(task, timeoutMs, TimeUnit.MILLISECONDS)
  }

  def bufferQueueInfo(): String = s"$this used buffers: ${bufferQueue.size()}"

  def stopAndCleanFlusher(): Unit = {
    stopFlag.set(true)
    try {
      workers.foreach(_.interrupt())
    } catch {
      case e: Exception =>
        logError(s"Exception when interrupt worker: ${workers.mkString(",")}, $e")
    }
    workingQueues.foreach { queue =>
      queue.asScala.foreach { task =>
        task.buffer.removeComponents(0, task.buffer.numComponents())
        task.buffer.clear()
      }
    }
  }

  def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit
}

private[worker] class LocalFlusher(
    workerSource: AbstractSource,
    val deviceMonitor: DeviceMonitor,
    threadCount: Int,
    val mountPoint: String,
    flushAvgTimeWindowSize: Int,
    flushAvgTimeMinimumCount: Int,
    val diskType: StorageInfo.Type) extends Flusher(
      workerSource,
      threadCount,
      flushAvgTimeWindowSize,
      flushAvgTimeMinimumCount)
      with DeviceObserver with Logging {

  deviceMonitor.registerFlusher(this)

  override def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit = {
    stopFlag.set(true)
    logError(s"$this write failed, report to DeviceMonitor, eception: $e")
    deviceMonitor.reportDeviceError(mountPoint, e, deviceErrorType)
  }

  override def notifyError(mountPoint: String,
                           diskStatus: DiskStatus): Unit = {
    logError(s"$this is notified Disk $mountPoint $diskStatus! Stop LocalFlusher.")
    stopAndCleanFlusher()
    deviceMonitor.unregisterFlusher(this)
  }

  override def hashCode(): Int = {
    mountPoint.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[LocalFlusher] &&
      obj.asInstanceOf[LocalFlusher].mountPoint.equals(mountPoint)
  }

  override def toString(): String = {
    s"LocalFlusher@$flusherId-$mountPoint"
  }
}

private[worker] final class HdfsFlusher(
    workerSource: AbstractSource,
    threadCount: Int,
    flushAvgTimeWindowSize: Int,
    flushAvgTimeMinimumCount: Int) extends Flusher(
      workerSource,
      threadCount,
      flushAvgTimeWindowSize,
      flushAvgTimeMinimumCount) with Logging {

  override def toString: String = s"HdfsFlusher@$flusherId"

  override def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit = {
    stopAndCleanFlusher()
    logError(s"$this write failed, reason $deviceErrorType ,exception: $e")
  }
}

private[worker] final class StorageManager(
    conf: RssConf,
    workerSource: AbstractSource)
  extends ShuffleRecoverHelper with DeviceObserver with Logging with MemoryTrackerListener{
  // mount point -> filewriter
  val workingDirWriters = new ConcurrentHashMap[File, util.ArrayList[FileWriter]]()

  val (deviceInfos, diskInfos) = {
    val workingDirInfos =
      RssConf.workerBaseDirs(conf).map { case (workdir, maxSpace, flusherThread, storageType) =>
        (new File(workdir, RssConf.WorkingDirName), maxSpace, flusherThread, storageType)
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
  var hdfsFs: FileSystem = _
  val hdfsPermission = FsPermission.createImmutable(755)
  val hdfsWriters = new util.ArrayList[FileWriter]()
  val hdfsFlusher = if (!hdfsDir.isEmpty) {
    val hdfsConfiguration = new Configuration
    hdfsConfiguration.set("fs.defaultFS", hdfsDir)
    hdfsConfiguration.set("dfs.replication", "1")
    hdfsFs = FileSystem.get(hdfsConfiguration)
    Some(new HdfsFlusher(
      workerSource,
      RssConf.hdfsFlusherThreadCount(conf),
      RssConf.flushAvgTimeWindow(conf),
      RssConf.flushAvgTimeMinimumCount(conf)))
  } else {
    None
  }

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

    val partitionId = location.getId
    val epoch = location.getEpoch
    val mode = location.getMode
    val fileName = s"$partitionId-$epoch-${mode.mode()}"

    var retryCount = 0
    var exception: IOException = null
    val suggestedMountPoint = location.getStorageHint.getMountPoint
    while (retryCount < RssConf.createFileWriterRetryCount(conf)) {
      val diskInfo = diskInfos.get(suggestedMountPoint)
      val dirs = if (diskInfo != null && diskInfo.status.equals(DiskStatus.Healthy)) {
        diskInfo.dirs
      } else {
        logWarning(s"Disk unavailable for $suggestedMountPoint, return all healthy" +
          " working dirs. diskInfo $diskInfo")
        healthyWorkingDirs()
      }
      if (dirs.isEmpty && hdfsFlusher.isEmpty) {
        throw new IOException(s"No available disks! suggested mountPoint $suggestedMountPoint")
      }
      val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
      if (dirs.isEmpty) {
        FileSystem.mkdirs(hdfsFs, new Path(s"$hdfsDir/$appId/$shuffleId"), hdfsPermission)
        val fileInfo = new FileInfo(FileSystem.create(hdfsFs,
          new Path(s"$hdfsDir/$appId/$shuffleId/$fileName"), hdfsPermission))
        val hdfsWriter = new FileWriter(
          fileInfo,
          hdfsFlusher.get,
          workerSource,
          conf,
          deviceMonitor,
          splitThreshold,
          splitMode,
          partitionType)
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
          val fileInfo = new FileInfo(file)
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
      fileInfos.remove(shuffleKey)
      val (appId, shuffleId) = Utils.splitShuffleKey(shuffleKey)
      disksSnapshot().filter(_.status != DiskStatus.IoHang).foreach{ case diskInfo =>
        diskInfo.dirs.foreach { case dir =>
          val file = new File(dir, s"$appId/$shuffleId")
          deleteDirectory(file, diskOperators.get(diskInfo.mountPoint))
        }
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

  val rssSlowFlushInterval: Long = RssConf.slowFlushIntervalMs(conf)

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
}
