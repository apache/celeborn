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

package org.apache.celeborn.tests.spark.fetch.failure

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.TaskContext
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ShuffleManagerHook, SparkCommonUtils, SparkShuffleManager, SparkUtils, TestCelebornShuffleManager}

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

class ShuffleReaderGetHooks(
    conf: CelebornConf,
    workerDirs: Seq[String],
    shuffleIdToBeDeleted: Seq[Int] = Seq(),
    triggerStageId: Option[Int] = None)
  extends ShuffleManagerHook {

  var executed: AtomicBoolean = new AtomicBoolean(false)
  val lock = new Object

  private def deleteDataFile(appUniqueId: String, celebornShuffleId: Int): Unit = {
    val datafile =
      workerDirs.map(dir => {
        new File(s"$dir/celeborn-worker/shuffle_data/$appUniqueId/$celebornShuffleId")
      }).filter(_.exists())
        .flatMap(_.listFiles().iterator).headOption
    datafile match {
      case Some(file) => {
        file.delete()
      }
      case None => throw new RuntimeException("unexpected, there must be some data file")
    }
  }

  override def exec(
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): Unit = {
    if (executed.get()) {
      return
    }
    lock.synchronized {
      handle match {
        case h: CelebornShuffleHandle[_, _, _] => {
          val appUniqueId = h.appUniqueId
          val shuffleClient = ShuffleClient.get(
            h.appUniqueId,
            h.lifecycleManagerHost,
            h.lifecycleManagerPort,
            conf,
            h.userIdentifier,
            h.extension)
          val celebornShuffleId = SparkUtils.celebornShuffleId(shuffleClient, h, context, false)
          val appShuffleIdentifier =
            SparkCommonUtils.encodeAppShuffleIdentifier(handle.shuffleId, context)
          val Array(_, stageId, _) = appShuffleIdentifier.split('-')
          if (triggerStageId.isEmpty || triggerStageId.get == stageId.toInt) {
            if (shuffleIdToBeDeleted.isEmpty) {
              deleteDataFile(appUniqueId, celebornShuffleId)
            } else {
              shuffleIdToBeDeleted.foreach { shuffleId =>
                deleteDataFile(appUniqueId, shuffleId)
              }
            }
            executed.set(true)
          }
        }
        case x => throw new RuntimeException(s"unexpected, only support RssShuffleHandle here," +
            s" but get ${x.getClass.getCanonicalName}")
      }
    }
  }
}
