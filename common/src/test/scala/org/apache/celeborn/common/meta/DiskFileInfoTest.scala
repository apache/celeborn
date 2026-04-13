package org.apache.celeborn.common.meta

import java.nio.file.Files

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.protocol.StorageInfo

class DiskFileInfoTest extends CelebornFunSuite {

  test("test diskFileInfoUsageAccounting positive + negative") {
    val usableSpace = 1000L
    val diskInfo = new DiskInfo(
      "SSD",
      usableSpace,
      Integer.MAX_VALUE,
      Integer.MAX_VALUE,
      Integer.MAX_VALUE,
      StorageInfo.Type.SSD)
    val tmpFilePath = Files.createTempFile("testDiskUsageAccounting", ".tmp")
    val diskFileInfo = new DiskFileInfo(
      diskInfo,
      new UserIdentifier("tenant1", "user1"),
      false,
      new ReduceFileMeta(8192),
      tmpFilePath.toString,
      StorageInfo.Type.SSD)

    diskFileInfo.updateBytesFlushed(100)
    assert(diskFileInfo.getFileLength == 100, "file length should be updated to flushed bytes")
    assert(
      diskInfo.getTransientAvailableBytes == (usableSpace - 100),
      "available bytes should be reduced by flushed bytes")

    try {
      diskFileInfo.updateBytesFlushed(901)
      fail("should throw IllegalStateException when flush bytes exceed usable space")
    } catch {
      case IllegalStateException =>
        assert(
          diskInfo.getTransientAvailableBytes == usableSpace - 100,
          "available bytes should not be reduced when flush bytes exceed usable space")
    }
    // The failed acquire should not affect the available bytes, and the successful acquire should reduce the available bytes
    assert(
      diskInfo.getTransientAvailableBytes == usableSpace - 100,
      "available bytes should be reduced by flushed bytes")

    // With null diskInfo, no exceptions during acquisition
    val diskFileInfo2 = new DiskFileInfo(
      null,
      new UserIdentifier("tenant1", "user1"),
      false,
      new ReduceFileMeta(8192),
      tmpFilePath.toString,
      StorageInfo.Type.SSD)
    diskFileInfo2.updateBytesFlushed(5000)
    assert(diskFileInfo2.getFileLength == 5000, "file length should be updated to flushed bytes")
    tmpFilePath.toFile.deleteOnExit()
  }

  test("Multi threaded acquisition") {
    val usableSpace = 1000L
    val diskInfo = new DiskInfo(
      "SSD",
      usableSpace,
      Integer.MAX_VALUE,
      Integer.MAX_VALUE,
      Integer.MAX_VALUE,
      StorageInfo.Type.SSD)
    val tmpFilePath = Files.createTempFile("testDiskUsageAccountingMultiThreaded", ".tmp")

    val failures = new java.util.concurrent.atomic.AtomicInteger(0)
    val totalSuccessfulAcquisition = new java.util.concurrent.atomic.AtomicInteger(0)
    val perThreadFlushBytes = 101

    val threads = (1 to 10).map { _ =>
      new Thread(new Runnable {
        override def run(): Unit = {
          try {
            val diskFileInfo = new DiskFileInfo(
              diskInfo,
              new UserIdentifier("tenant1", "user1"),
              false,
              new ReduceFileMeta(8192),
              tmpFilePath.toString,
              StorageInfo.Type.SSD)
            diskFileInfo.updateBytesFlushed(perThreadFlushBytes)
            totalSuccessfulAcquisition.addAndGet(perThreadFlushBytes)
          } catch {
            case _: IllegalStateException =>
              failures.incrementAndGet()
            // expected when flush bytes exceed usable space
          }
        }
      })
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    // Only the first 6 threads should succeed in flushing 150 bytes each (total 900 bytes), and the rest should fail due to insufficient space
    assert(
      diskInfo.getTransientAvailableBytes == (usableSpace - totalSuccessfulAcquisition.get()),
      "available bytes should be reduced by flushed bytes")
    assert(diskInfo.getTransientAvailableBytes > 0, "available bytes should not be negative")
    assert(failures.get() == 1, "1 threads should fail due to insufficient space")
  }

}
