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

import java.io.File
import java.util.{ArrayList => jArrayList}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.junit.Assert.assertEquals
import org.mockito.ArgumentMatchers._
import org.mockito.MockitoSugar._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.WORKER_DISK_MONITOR_CHECK_INTERVAL
import org.apache.celeborn.common.meta.{DeviceInfo, DiskInfo, DiskStatus}
import org.apache.celeborn.common.protocol.StorageInfo
import org.apache.celeborn.common.util.Utils

class DeviceMonitorSuite extends AnyFunSuite {
  val dfCmd = "df -ah"
  val dfOut =
    """
      |Filesystem      Size  Used Avail  Use% Mounted on
      |devtmpfs         32G     0   32G    0% /dev
      |tmpfs            32G  108K   32G    1% /dev/shm
      |tmpfs            32G  672K   32G    1% /run
      |tmpfs            32G     0   32G    0% /sys/fs/cgroup
      |/dev/vda1       118G   43G   71G   38% /
      |tmpfs           6.3G     0  6.3G    0% /run/user/0
      |/dev/vda        1.8T   91G  1.7T    6% /mnt/disk1
      |/dev/vdb        1.8T   91G  1.7T    6% /mnt/disk2
      |tmpfs           6.3G     0  6.3G    0% /run/user/1001
      |""".stripMargin

  val lsCmd = "ls /sys/block/"
  val lsOut = "loop0  loop1  loop2  loop3  loop4  loop5  loop6  loop7  vda  vdb"

  val dirs = new jArrayList[File]()
  val workingDir1 = ListBuffer[File](new File("/mnt/disk1/data1"))
  val workingDir2 = ListBuffer[File](new File("/mnt/disk1/data2"))
  val workingDir3 = ListBuffer[File](new File("/mnt/disk2/data3"))
  val workingDir4 = ListBuffer[File](new File("/mnt/disk2/data4"))
  dirs.addAll(workingDir1.asJava)
  dirs.addAll(workingDir2.asJava)
  dirs.addAll(workingDir3.asJava)
  dirs.addAll(workingDir4.asJava)

  val conf = new CelebornConf()
  conf.set(WORKER_DISK_MONITOR_CHECK_INTERVAL.key, "3600s")

  val storageManager = mock[DeviceObserver]
  var (deviceInfos, diskInfos, workingDirDiskInfos): (
      java.util.Map[String, DeviceInfo],
      java.util.Map[String, DiskInfo],
      java.util.Map[String, DiskInfo]) = (null, null, null)

  withObjectMocked[org.apache.celeborn.common.util.Utils.type] {
    when(Utils.runCommand(dfCmd)) thenReturn dfOut
    when(Utils.runCommand(lsCmd)) thenReturn lsOut
    val (tdeviceInfos, tdiskInfos) = DeviceInfo.getDeviceAndDiskInfos(
      dirs.asScala.toArray.map(f =>
        (f, Long.MaxValue, 1, StorageInfo.Type.HDD)),
      conf)
    deviceInfos = tdeviceInfos
    diskInfos = tdiskInfos
  }
  val deviceMonitor =
    new LocalDeviceMonitor(conf, storageManager, deviceInfos, diskInfos)

  val vdaDeviceInfo = new DeviceInfo("vda")
  val vdbDeviceInfo = new DeviceInfo("vdb")

  test("init") {
    withObjectMocked[org.apache.celeborn.common.util.Utils.type] {
      when(Utils.runCommand(dfCmd)) thenReturn dfOut
      when(Utils.runCommand(lsCmd)) thenReturn lsOut

      deviceMonitor.init()

      assertEquals(2, deviceMonitor.observedDevices.size())

      assert(deviceMonitor.observedDevices.containsKey(vdaDeviceInfo))
      assert(deviceMonitor.observedDevices.containsKey(vdbDeviceInfo))

      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).diskInfos.size, 1)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).diskInfos.size, 1)

      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).diskInfos.containsKey("/mnt/disk1"))
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).diskInfos.containsKey("/mnt/disk2"))

      assertEquals(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).diskInfos.get("/mnt/disk1").dirs(0),
        new File("/mnt/disk1/data1"))
      assertEquals(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).diskInfos.get("/mnt/disk1").dirs(1),
        new File("/mnt/disk1/data2"))
      assertEquals(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).diskInfos.get("/mnt/disk2").dirs(0),
        new File("/mnt/disk2/data3"))
      assertEquals(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).diskInfos.get("/mnt/disk2").dirs(1),
        new File("/mnt/disk2/data4"))

      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 1)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 1)
    }
  }

  test("register/unregister/notify/report") {
    withObjectMocked[org.apache.celeborn.common.util.Utils.type] {
      when(Utils.runCommand(dfCmd)) thenReturn dfOut
      when(Utils.runCommand(lsCmd)) thenReturn lsOut

      deviceMonitor.init()

      val fw1 = mock[FileWriter]
      val fw2 = mock[FileWriter]
      val fw3 = mock[FileWriter]
      val fw4 = mock[FileWriter]

      val f1 = new File("/mnt/disk1/data1/f1")
      val f2 = new File("/mnt/disk1/data2/f2")
      val f3 = new File("/mnt/disk2/data3/f3")
      val f4 = new File("/mnt/disk2/data4/f4")
      when(fw1.getFile).thenReturn(f1)
      when(fw2.getFile).thenReturn(f2)
      when(fw3.getFile).thenReturn(f3)
      when(fw4.getFile).thenReturn(f4)

      deviceMonitor.registerFileWriter(fw1)
      deviceMonitor.registerFileWriter(fw2)
      deviceMonitor.registerFileWriter(fw3)
      deviceMonitor.registerFileWriter(fw4)

      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 3)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 3)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(fw1))
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(fw2))
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(fw3))
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(fw4))

      deviceMonitor.unregisterFileWriter(fw1)
      deviceMonitor.unregisterFileWriter(fw3)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 2)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 2)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(fw2))
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(fw4))

      val df1 = mock[LocalFlusher]
      val df2 = mock[LocalFlusher]
      val df3 = mock[LocalFlusher]
      val df4 = mock[LocalFlusher]

      when(df1.stopFlag).thenReturn(new AtomicBoolean(false))
      when(df2.stopFlag).thenReturn(new AtomicBoolean(false))
      when(df3.stopFlag).thenReturn(new AtomicBoolean(false))
      when(df4.stopFlag).thenReturn(new AtomicBoolean(false))

      when(df1.mountPoint).thenReturn("/mnt/disk1")
      when(df2.mountPoint).thenReturn("/mnt/disk1")
      when(df3.mountPoint).thenReturn("/mnt/disk2")
      when(df4.mountPoint).thenReturn("/mnt/disk2")

      deviceMonitor.registerFlusher(df1)
      deviceMonitor.registerFlusher(df2)
      deviceMonitor.registerFlusher(df3)
      deviceMonitor.registerFlusher(df4)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 4)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 4)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(df1))
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(df2))
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(df3))
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(df4))

      deviceMonitor.unregisterFlusher(df1)
      deviceMonitor.unregisterFlusher(df3)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 3)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 3)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(df2))
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(df4))

      when(fw2.notifyError("vda", DiskStatus.IO_HANG))
        .thenAnswer((a: String, b: List[File]) => {
          deviceMonitor.unregisterFileWriter(fw2)
        })
      when(fw4.notifyError("vdb", DiskStatus.IO_HANG))
        .thenAnswer((a: String, b: List[File]) => {
          deviceMonitor.unregisterFileWriter(fw4)
        })
      when(df2.notifyError("vda", DiskStatus.IO_HANG))
        .thenAnswer((a: String, b: List[File]) => {
          df2.stopFlag.set(true)
        })
      when(df4.notifyError("vdb", DiskStatus.IO_HANG))
        .thenAnswer((a: String, b: List[File]) => {
          df4.stopFlag.set(true)
        })

      deviceMonitor.observedDevices
        .get(vdaDeviceInfo)
        .notifyObserversOnError(List("/mnt/disk1"), DiskStatus.IO_HANG)
      deviceMonitor.observedDevices
        .get(vdbDeviceInfo)
        .notifyObserversOnError(List("/mnt/disk2"), DiskStatus.IO_HANG)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 3)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 3)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(df2))
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(df4))

      deviceMonitor.registerFileWriter(fw1)
      deviceMonitor.registerFileWriter(fw2)
      deviceMonitor.registerFileWriter(fw3)
      deviceMonitor.registerFileWriter(fw4)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 4)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 4)
      val dirs = new jArrayList[File]()
      dirs.add(null)
      when(fw1.notifyError(any(), any()))
        .thenAnswer((_: Any) => {
          deviceMonitor.unregisterFileWriter(fw1)
        })
      when(fw2.notifyError(any(), any()))
        .thenAnswer((_: Any) => {
          deviceMonitor.unregisterFileWriter(fw2)
        })
      deviceMonitor.reportDeviceError("/mnt/disk1", null, DiskStatus.IO_HANG)
      assertEquals(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.size(), 2)
      assert(
        deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdaDeviceInfo).observers.contains(df2))

      when(fw3.notifyError(any(), any()))
        .thenAnswer((_: Any) => {
          deviceMonitor.unregisterFileWriter(fw3)
        })
      when(fw4.notifyError(any(), any()))
        .thenAnswer((_: Any) => {
          deviceMonitor.unregisterFileWriter(fw4)
        })
      deviceMonitor.reportDeviceError("/mnt/disk2", null, DiskStatus.IO_HANG)
      assertEquals(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.size(), 2)
      assert(
        deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(storageManager))
      assert(deviceMonitor.observedDevices.get(vdbDeviceInfo).observers.contains(df4))
    }
  }

  test("tryWithTimeoutAndCallback") {
    val fn = (i: Int) => {
      0 until 100 foreach (x => {
        // scalastyle:off println
        println(i + Thread.currentThread().getName)
        Thread.sleep(2000)
        // scalastyle:on println
      })
      true
    }
    0 until 3 foreach (i => {
      val result = Utils.tryWithTimeoutAndCallback({
        fn(i)
      })(false)(DeviceMonitor.deviceCheckThreadPool, 1)
      assert(!result)
    })
    DeviceMonitor.deviceCheckThreadPool.shutdownNow()
  }
}
