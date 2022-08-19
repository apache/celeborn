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

import java.io.File

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.network.server.FileInfo
import com.aliyun.emr.rss.common.util.Utils
import org.junit.Assert
import org.scalatest.funsuite.AnyFunSuite

class StorageManagerSuite extends AnyFunSuite {
  test("Recover from LevelDB") {
    val recoverPath = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "recover_path")
    val baseDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "base_dir")
    val conf: RssConf = new RssConf
    conf.set("rss.worker.base.dirs", baseDir.getPath)
    conf.set("rss.worker.graceful.shutdown", "true")
    conf.set("rss.worker.recoverPath", recoverPath.getPath)
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    storageManager.updateShuffleFileInfos(
      "application-1-1", "0-0-1", new FileInfo(new File("/tmp/file1")))
    storageManager.updateShuffleFileInfos(
      "application-1-1", "0-0-2", new FileInfo(new File("/tmp/file2")))
    storageManager.updateShuffleFileInfos(
      "application-2-1", "0-0-1", new FileInfo(new File("/tmp/file3")))
    storageManager.updateShuffleFileInfos(
      "application-2-1", "0-0-2", new FileInfo(new File("/tmp/file4")))
    storageManager.updateShuffleFileInfos(
      "application-3-1", "0-0-1", new FileInfo(new File("/tmp/file5")))
    storageManager.close()
    val storageManager2 = new StorageManager(conf, new WorkerSource(conf))
    Assert.assertEquals(
      storageManager2.getFileInfo("application-1-1", "0-0-1").toString, "/tmp/file1")
    Assert.assertEquals(
      storageManager2.getFileInfo("application-1-1", "0-0-2").toString, "/tmp/file2")
    Assert.assertEquals(
      storageManager2.getFileInfo("application-2-1", "0-0-1").toString, "/tmp/file3")
    Assert.assertEquals(
      storageManager2.getFileInfo("application-2-1", "0-0-2").toString, "/tmp/file4")
    Assert.assertEquals(
      storageManager2.getFileInfo("application-3-1", "0-0-1").toString, "/tmp/file5")
    storageManager2.close()
    recoverPath.delete()
    baseDir.delete()
  }
}
