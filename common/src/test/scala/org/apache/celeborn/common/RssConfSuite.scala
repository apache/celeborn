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

import org.apache.celeborn.RssFunSuite
import org.apache.celeborn.common.RssConf.{haMasterHosts, masterPort}

class RssConfSuite extends RssFunSuite {

  test("issue 26: rss.master.address support multi host") {
    val conf = new RssConf()
    conf.set("rss.master.address", "localhost1:9097,localhost2:9097")
    assert("localhost1,localhost2" == haMasterHosts(conf))
    assert(9097 == masterPort(conf))
  }

  test("basedir test") {
    val conf = new RssConf()
    val defaultMaxUsableSpace = 1024L * 1024 * 1024 * 1024 * 1024
    conf.set("rss.worker.base.dirs", "/mnt/disk1")
    val parsedDirs = RssConf.workerBaseDirs(conf)
    assert(parsedDirs.size == 1)
    assert(parsedDirs.head._3 == 1)
    assert(parsedDirs.head._2 == defaultMaxUsableSpace)
  }

  test("basedir test2") {
    val conf = new RssConf()
    val defaultMaxUsableSpace = 1024L * 1024 * 1024 * 1024 * 1024
    conf.set("rss.worker.base.dirs", "/mnt/disk1:disktype=SSD:capacity=10g")
    val parsedDirs = RssConf.workerBaseDirs(conf)
    assert(parsedDirs.size == 1)
    assert(parsedDirs.head._3 == 8)
    assert(parsedDirs.head._2 == 10 * 1024 * 1024 * 1024L)
  }

  test("basedir test3") {
    val conf = new RssConf()
    conf.set("rss.worker.base.dirs", "/mnt/disk1:disktype=SSD:capacity=10g:flushthread=3")
    val parsedDirs = RssConf.workerBaseDirs(conf)
    assert(parsedDirs.size == 1)
    assert(parsedDirs.head._3 == 3)
    assert(parsedDirs.head._2 == 10 * 1024 * 1024 * 1024L)
  }

  test("basedir test4") {
    val conf = new RssConf()
    conf.set(
      "rss.worker.base.dirs",
      "/mnt/disk1:disktype=SSD:capacity=10g:flushthread=3," +
        "/mnt/disk2:disktype=HDD:capacity=15g:flushthread=7")
    val parsedDirs = RssConf.workerBaseDirs(conf)
    assert(parsedDirs.size == 2)
    assert(parsedDirs.head._1 == "/mnt/disk1")
    assert(parsedDirs.head._3 == 3)
    assert(parsedDirs.head._2 == 10 * 1024 * 1024 * 1024L)

    assert(parsedDirs(1)._1 == "/mnt/disk2")
    assert(parsedDirs(1)._3 == 7)
    assert(parsedDirs(1)._2 == 15 * 1024 * 1024 * 1024L)
  }

  test("zstd level") {
    val conf = new RssConf()
    conf.set("rss.client.compression.zstd.level", "-100")
    assert(RssConf.zstdCompressLevel(conf) == -5)
    conf.set("rss.client.compression.zstd.level", "-5")
    assert(RssConf.zstdCompressLevel(conf) == -5)
    conf.set("rss.client.compression.zstd.level", "0")
    assert(RssConf.zstdCompressLevel(conf) == 0)
    conf.set("rss.client.compression.zstd.level", "22")
    assert(RssConf.zstdCompressLevel(conf) == 22)
    conf.set("rss.client.compression.zstd.level", "100")
    assert(RssConf.zstdCompressLevel(conf) == 22)
  }
}
