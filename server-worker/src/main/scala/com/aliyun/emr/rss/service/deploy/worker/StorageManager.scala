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
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.buffer.{CompositeByteBuf, Unpooled}
import org.iq80.leveldb.DB

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.exception.RssException
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.DeviceInfo
import com.aliyun.emr.rss.common.metrics.source.AbstractSource
import com.aliyun.emr.rss.common.network.server.{FileInfo, MemoryTracker}
import com.aliyun.emr.rss.common.network.server.MemoryTracker.MemoryTrackerListener
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.rss.common.utils.PBSerDeUtils
import com.aliyun.emr.rss.service.deploy.worker.FileWriter.FlushNotifier

trait DeviceObserver {
  def notifyError(deviceName: String, dirs: ListBuffer[File],
    deviceErrorType: DeviceErrorType): Unit = {}
  def notifyHealthy(dirs: ListBuffer[File]): Unit = {}
  def notifyHighDiskUsage(dirs: ListBuffer[File]): Unit = {}
  def notifySlowFlush(dirs: ListBuffer[File]): Unit = {}
  def reportError(workingDir: mutable.Buffer[File], e: IOException,
    deviceErrorType: DeviceErrorType): Unit = {}
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
                    processIOException(e, DeviceErrorType.ReadOrWriteFailure)
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
    logInfo(s"flushCount $flushCount")
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

  def processIOException(e: IOException, deviceErrorType: DeviceErrorType): Unit
}

private[worker] class LocalFlusher(
    val workingDirs: mutable.Buffer[File],
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

  override def processIOException(e: IOException, deviceErrorType: DeviceErrorType): Unit = {
    stopFlag.set(true)
    logError(s"$this write failed, report to DeviceMonitor, eception: $e")
    deviceMonitor.reportDeviceError(workingDirs, e, deviceErrorType)
  }

  override def notifyError(deviceName: String, dirs: ListBuffer[File] = null,
    deviceErrorType: DeviceErrorType): Unit = {
    logError(s"$this is notified Device $deviceName Error $deviceErrorType! Stop LocalFlusher.")
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
    deviceMonitor.unregisterFlusher(this)
  }

  override def reportError(workingDir: mutable.Buffer[File], e: IOException,
    deviceErrorType: DeviceErrorType): Unit = {
    deviceMonitor.reportDeviceError(workingDir, e, deviceErrorType)
  }

  override def hashCode(): Int = {
    workingDirs.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[LocalFlusher] &&
      obj.asInstanceOf[LocalFlusher].workingDirs.equals(workingDirs)
  }

  override def toString(): String = {
    s"LocalFlusher@$flusherId-" + workingDirs.toString
  }
}

private[worker] final class StorageManager(
    conf: RssConf,
    workerSource: AbstractSource)
  extends ShuffleFileRecoverHelper with DeviceObserver with Logging with MemoryTrackerListener{

  private val diskMinimumReserveSize = RssConf.diskMinimumReserveSize(conf)
  val isolatedWorkingDirs =
    new ConcurrentHashMap[File, DeviceErrorType](RssConf.workerBaseDirs(conf).length)

  private val workingDirMetas: mutable.HashMap[String, (Long, Int, StorageInfo.Type)] =
    new mutable.HashMap[String, (Long, Int, StorageInfo.Type)]()
  // mount point -> filewriter
  val workingDirWriters = new ConcurrentHashMap[File, util.ArrayList[FileWriter]]()

  private val workingDirs: util.ArrayList[File] = {
    val baseDirs =
      RssConf.workerBaseDirs(conf).map { case (workdir, maxSpace, flusherThread, storageHint) =>
        val actualWorkingDirFile = new File(workdir, RssConf.WorkingDirName)
        workingDirMetas +=
          actualWorkingDirFile.getAbsolutePath -> (maxSpace, flusherThread, storageHint)
        (actualWorkingDirFile, maxSpace, flusherThread, storageHint)
      }
    val availableDirs = new mutable.HashSet[File]()

    baseDirs.foreach { case (file, _, _, _) =>
      if (!DeviceMonitor.checkDiskReadAndWrite(conf, ListBuffer[File](file))) {
        availableDirs += file
      } else {
        logWarning(s"Exception when trying to create a file in $file, add to blacklist.")
        isolatedWorkingDirs.put(file, DeviceErrorType.ReadOrWriteFailure)
      }
    }
    if (availableDirs.size <= 0) {
      throw new IOException("No available working directory.")
    }
    new util.ArrayList[File](availableDirs.asJava)
  }

  def workingDirsSnapshot(): util.ArrayList[File] =
    workingDirs.synchronized(new util.ArrayList[File](workingDirs))

  def hasAvailableWorkingDirs(): Boolean = workingDirsSnapshot().size() > 0

  val writerFlushBufferSize: Long = RssConf.workerFlushBufferSize(conf)

  private val dirOperators: ConcurrentHashMap[File, ThreadPoolExecutor] = {
    val cleaners = new ConcurrentHashMap[File, ThreadPoolExecutor]()
    workingDirsSnapshot().asScala.foreach {
      dir => cleaners.put(dir,
        ThreadUtils.newDaemonCachedThreadPool(s"Disk-cleaner-${dir.getAbsoluteFile}", 1))
    }
    cleaners
  }

  val (deviceInfos, diskInfos, workingDirDiskInfos) =
    DeviceInfo.getDeviceAndDiskInfos(workingDirsSnapshot())
  private val deviceMonitor = DeviceMonitor.createDeviceMonitor(conf, this, deviceInfos, diskInfos)

  private val localFlushers: ConcurrentHashMap[File, LocalFlusher] = {
    val flushers = new ConcurrentHashMap[File, LocalFlusher]()
    workingDirsSnapshot().asScala.groupBy { dir =>
      workingDirDiskInfos.get(dir.getAbsolutePath).mountPointFile
    }.foreach { case (mountPointFile, dirs) =>
      if (!flushers.containsKey(mountPointFile)) {
        val firstWorkingDirPath = dirs.head.getAbsolutePath
        val flusher = new LocalFlusher(
          dirs,
          workerSource,
          deviceMonitor,
          workingDirMetas(firstWorkingDirPath)._2,
          mountPointFile.getAbsolutePath,
          RssConf.flushAvgTimeWindow(conf),
          RssConf.flushAvgTimeMinimumCount(conf),
          workingDirMetas(firstWorkingDirPath)._3
        )
        flushers.put(mountPointFile, flusher)
      }
    }
    flushers
  }

  private val actionService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
    .setNameFormat("StorageManager-action-thread").build)

  deviceMonitor.startCheck()

  def getUsableWorkingDirs(mountPoint: String): util.ArrayList[File] = {
    if (mountPoint != StorageInfo.UNKNOWN_DISK
      && diskInfos.get(mountPoint).usableSpace > diskMinimumReserveSize) {
      new util.ArrayList[File](diskInfos.get(mountPoint)
        .dirInfos.filter(workingDirs.contains(_)).toList.asJava)
    } else {
      logDebug(s"mount point $mountPoint is invalid or run out of space")
      new util.ArrayList[File](workingDirsSnapshot().asScala.filter { dir =>
        workingDirDiskInfos.get(dir.getAbsolutePath).usableSpace > diskMinimumReserveSize
      }.toList.asJava)
    }
  }

  override def notifyError(deviceName: String, dirs: ListBuffer[File],
    deviceErrorType: DeviceErrorType): Unit = this.synchronized {
    // add to isolatedWorkingDirs and decrease current slots and report
    dirs.foreach(dir => {
      workingDirs.synchronized {
        workingDirs.remove(dir)
      }

      if (deviceErrorType == DeviceErrorType.IoHang) {
        logInfo("IoHang, remove dir operator")
        val operator = dirOperators.remove(dir)
        if (operator != null) {
          operator.shutdown()
        }
      }

      val flusher = localFlushers.get(workingDirDiskInfos.get(dir.getAbsolutePath).mountPointFile)
      if (flusher == null) {
        // this branch means this localFlusher is already removed
        isolatedWorkingDirs.put(dir, deviceErrorType)
      } else {
        flusher.workingDirs.foreach(dir =>
          isolatedWorkingDirs.put(dir, deviceErrorType))
      }
    })
  }

  override def notifyHealthy(dirs: ListBuffer[File]): Unit = this.synchronized {
    dirs.groupBy(file => workingDirDiskInfos.get(file.getAbsolutePath).mountPointFile)
      .foreach { case (mountPointFile, dirs) =>
        for (dir <- dirs) {
          isolatedWorkingDirs.remove(dir)
        }
        if (!localFlushers.containsKey(mountPointFile)) {
          val flusher = new LocalFlusher(
            dirs,
            workerSource,
            deviceMonitor,
            workingDirMetas(dirs.head.getAbsolutePath)._2,
            diskInfos.get(dirs.head.getAbsolutePath).mountPoint,
            RssConf.flushAvgTimeWindow(conf),
            RssConf.flushAvgTimeMinimumCount(conf),
            workingDirMetas(dirs.head.getAbsolutePath)._3
          )
          localFlushers.put(mountPointFile, flusher)
        } else {
          localFlushers.get(mountPointFile).workingDirs ++= dirs
        }
        for (dir <- dirs) {
          if (!dirOperators.containsKey(dir)) {
            dirOperators.put(dir,
              ThreadUtils.newDaemonCachedThreadPool(s"Disk-cleaner-${dir.getAbsoluteFile}", 1))
          }
          workingDirs.synchronized {
            if (!workingDirs.contains(dir)) {
              workingDirs.add(dir)
            }
          }
        }
      }
    dirs.foreach(dir => {
      isolatedWorkingDirs.remove(dir)
      if (!localFlushers.containsKey(workingDirDiskInfos.get(dir.getAbsolutePath).mountPointFile)) {
        val flusher = new LocalFlusher(
          dirs,
          workerSource,
          deviceMonitor,
          workingDirMetas(dir.getAbsolutePath)._2,
          diskInfos.get(dir.getAbsolutePath).mountPoint,
          RssConf.flushAvgTimeWindow(conf),
          RssConf.flushAvgTimeMinimumCount(conf),
          workingDirMetas(dir.getAbsolutePath)._3
        )
        localFlushers.put(dir, flusher)
      }
      if (!dirOperators.containsKey(dir)) {
        dirOperators.put(dir,
          ThreadUtils.newDaemonCachedThreadPool(s"Disk-cleaner-${dir.getAbsoluteFile}", 1))
      }
      workingDirs.synchronized{
        if (!workingDirs.contains(dir)) {
          workingDirs.add(dir)
        }
      }
    })
  }

  def isolateDirs(dirs: ListBuffer[File], errorType: DeviceErrorType): Unit = this.synchronized {
    dirs.foreach { dir =>
      workingDirs.synchronized {
        workingDirs.remove(dir)
      }
      isolatedWorkingDirs.put(dir, errorType)
    }
  }

  override def notifyHighDiskUsage(dirs: ListBuffer[File]): Unit = {
    isolateDirs(dirs, DeviceErrorType.InsufficientDiskSpace)
  }

  override def notifySlowFlush(dirs: ListBuffer[File]): Unit = {
    isolateDirs(dirs, DeviceErrorType.FlushTimeout)
  }

  private val fetchChunkSize = RssConf.workerFetchChunkSize(conf)

  private val counter = new AtomicInteger()
  private val counterOperator = new IntUnaryOperator() {
    override def applyAsInt(operand: Int): Int = {
      val dirs = workingDirsSnapshot()
      if (dirs.size() > 0) {
        (operand + 1) % dirs.size()
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
  } else {
    this.fileInfosDb = null
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
            logInfo(s"Reload $shuffleKey -> $files")
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

  def updateShuffleFileInfos(shuffleKey: String, fileName: String, fileInfo: FileInfo): Unit = {
    val shuffleMap = fileInfos.computeIfAbsent(shuffleKey, newMapFunc)
    shuffleMap.put(fileName, fileInfo)
  }

  private def getNextIndex() = counter.getAndUpdate(counterOperator)

  private val newMapFunc =
    new java.util.function.Function[String, ConcurrentHashMap[String, FileInfo]]() {
      override def apply(key: String): ConcurrentHashMap[String, FileInfo] =
        new ConcurrentHashMap()
    }

  def numDisks(): Int = workingDirs.synchronized {
    workingDirs.size()
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
    if (!hasAvailableWorkingDirs()) {
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
      val dirs = getUsableWorkingDirs(suggestedMountPoint)
      if (dirs.isEmpty) {
        logDebug("All disks are not available.")
        dirs.addAll(workingDirsSnapshot())
      }
      val index = getNextIndex()
      val dir = dirs.get(index % dirs.size())
      val shuffleDir = new File(dir, s"$appId/$shuffleId")
      val file = new File(shuffleDir, fileName)

      try {
        shuffleDir.mkdirs()
        val createFileSuccess = file.createNewFile()
        if (!createFileSuccess) {
          throw new RssException("create app shuffle data dir or file failed")
        }
        val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
        val fileInfo = new FileInfo(file)
        val fileWriter = new FileWriter(
          fileInfo,
          localFlushers.get(workingDirDiskInfos.get(dir.getAbsolutePath).mountPointFile),
          fetchChunkSize,
          writerFlushBufferSize,
          workerSource,
          conf,
          deviceMonitor,
          splitThreshold,
          splitMode,
          partitionType)
        deviceMonitor.registerFileWriter(fileWriter)
        val list = workingDirWriters.computeIfAbsent(dir, workingDirWriterListFunc)
        list.synchronized {list.add(fileWriter)}
        fileWriter.registerDestroyHook(list)
        updateShuffleFileInfos(shuffleKey, fileName, fileInfo)
        location.getStorageHint.setMountPoint(
          workingDirDiskInfos.get(dir.getAbsolutePath).mountPoint)
        logDebug(s"location $location set disk hint to ${location.getStorageHint} ")
        return fileWriter
      } catch {
        case t: Throwable =>
          logError("Create Writer failed, report to DeviceMonitor", t)
          exception = new IOException(t)
          deviceMonitor.reportDeviceError(dirs.asScala, exception,
            DeviceErrorType.ReadOrWriteFailure)
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
    val workingDirs = workingDirsSnapshot()
    workingDirs.addAll(isolatedWorkingDirs.asScala.filterNot(entry => {
      entry._2 == DeviceErrorType.IoHang
    }).keySet.asJava)

    expiredShuffleKeys.asScala.foreach { shuffleKey =>
      logInfo(s"Cleanup expired shuffle $shuffleKey.")
      fileInfos.remove(shuffleKey)
      val (appId, shuffleId) = Utils.splitShuffleKey(shuffleKey)
      workingDirs.asScala.foreach { workingDir =>
        val dir = new File(workingDir, s"$appId/$shuffleId")
        deleteDirectory(dir, dirOperators.get(workingDir))
      }
    }
  }

  private val noneEmptyDirExpireDurationMs = RssConf.noneEmptyDirExpireDurationMs(conf)
  private val noneEmptyDirCleanUpThreshold = RssConf.noneEmptyDirCleanUpThreshold(conf)
  private val emptyDirExpireDurationMs = RssConf.emptyDirExpireDurationMs(conf)

  private val storageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("storage-scheduler")

  storageScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      try {
        // Clean up empty dirs, since the appDir do not delete during expired shuffle key cleanup
        cleanupExpiredAppDirs(expireDuration =
          System.currentTimeMillis() - emptyDirExpireDurationMs)

        // Clean up non-empty dirs which has not been modified
        // in the past {{noneEmptyExpireDurationsMs}}, since
        // non empty dirs may exist after cluster restart.
        cleanupExpiredAppDirs(deleteRecursively = true,
          System.currentTimeMillis() - noneEmptyDirExpireDurationMs)
      } catch {
        case exception: Exception =>
          logWarning(s"Cleanup expired shuffle data exception: ${exception.getMessage}")
      }
    }
  }, 30, 30, TimeUnit.MINUTES)

  val rssSlowFlushInterval: Long = RssConf.slowFlushIntervalMs(conf)
  storageScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      val currentTime = System.nanoTime()
      localFlushers.values().asScala.foreach(flusher => {
        logDebug(flusher.bufferQueueInfo())
        val lastFlushTimes = flusher.lastBeginFlushTime
        for (index <- 1 until lastFlushTimes.length()) {
          if (lastFlushTimes.get(index) != -1 &&
            currentTime - lastFlushTimes.get(index) > rssSlowFlushInterval * 1000 * 1000) {
            flusher.reportError(flusher.workingDirs, new IOException("Slow LocalFlusher!"),
              DeviceErrorType.FlushTimeout)
          }
        }
      })
    }
  }, rssSlowFlushInterval, rssSlowFlushInterval, TimeUnit.MILLISECONDS)

  private def cleanupExpiredAppDirs(
      deleteRecursively: Boolean = false, expireDuration: Long): Unit = {
    val workingDirs = workingDirsSnapshot()
    workingDirs.addAll(isolatedWorkingDirs.asScala.filterNot(entry => {
      entry._2 == DeviceErrorType.IoHang
    }).keySet.asJava)

    workingDirs.asScala.foreach { workingDir =>
      var appDirs = workingDir.listFiles

      if (appDirs != null) {
        if (deleteRecursively) {
          appDirs = appDirs.sortBy(_.lastModified()).take(noneEmptyDirCleanUpThreshold)
          for (appDir <- appDirs if appDir.lastModified() < expireDuration) {
            val executionThreadPool = dirOperators.get(workingDir)
            deleteDirectory(appDir, executionThreadPool)
            logInfo(s"Deleted expired app dirs $appDir.")
          }
        } else {
          for (appDir <- appDirs if appDir.lastModified() < expireDuration) {
            if (appDir.list().isEmpty) {
              deleteFileWithRetry(appDir)
            }
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
        logInfo(s"Deleted expired app dirs $file.")
      } else {
        logWarning(s"Delete expired app dirs $file failed.")
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
    if (null != dirOperators) {
      dirOperators.asScala.foreach(entry => {
        entry._2.shutdownNow()
      })
    }
    storageScheduler.shutdownNow()
    if (null != deviceMonitor) {
      deviceMonitor.close()
    }
  }

  private def flushFileWriters(): Unit = {
    workingDirWriters.asScala.foreach { case (file, writers) =>
      if (writers != null && writers.size() > 0) {
        writers.asScala.foreach { case writer =>
          writer.flushOnMemoryPressure()
        }
      }
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
    diskInfos.asScala.foreach { case (_, diskInfo) =>
      val dirInfos = diskInfo.dirInfos.toList
      val totalUsage = dirInfos.map { dir =>
        val writers = workingDirWriters.get(dir)
        if (writers != null && writers.size() > 0) {
          writers.asScala.map(_.getFileInfo.getFileLength).sum
        } else {
          0
        }
      }.sum
      val totalConfiguredCapacity = dirInfos
        .map(file => workingDirMetas(file.getAbsolutePath)._1).sum
      val fileSystemReportedUsableSpace = Files.getFileStore(
        Paths.get(diskInfo.mountPointFile.getPath)).getUsableSpace
      val workingDirUsableSpace = Math.min(totalConfiguredCapacity - totalUsage,
        fileSystemReportedUsableSpace)
      val flushTimeAverage = localFlushers.get(diskInfo.mountPointFile).averageFlushTime()
      diskInfo.update(workingDirUsableSpace, flushTimeAverage)
    }
    logInfo(s"Updated diskInfos: $diskInfos")
  }
}
