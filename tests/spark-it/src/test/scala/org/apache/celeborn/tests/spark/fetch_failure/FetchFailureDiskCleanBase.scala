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

package org.apache.celeborn.tests.spark.fetch_failure

import java.io.File

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.service.deploy.worker.Worker
import org.apache.celeborn.spark.FailedShuffleCleaner

private[tests] trait FetchFailureDiskCleanBase extends AnyFunSuite
  with FetchFailureTestBase
  with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    setupMiniClusterWithRandomPorts(workerNum = 1)
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
    FailedShuffleCleaner.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  override def createWorker(map: Map[String, String]): Worker = {
    val storageDir = createTmpDir()
    workerDirs = workerDirs :+ storageDir
    super.createWorker(map ++ Map("celeborn.master.heartbeat.worker.timeout" -> "10s"), storageDir)
  }

  class CheckingThread(
      shuffleIdShouldNotExist: Seq[Int],
      shuffleIdMustExist: Seq[Int],
      sparkSession: SparkSession)
    extends Thread {
    var exception: Exception = _

    protected def checkDirStatus(): Boolean = {
      val deletedSuccessfully = shuffleIdShouldNotExist.forall(shuffleId => {
        workerDirs.forall(dir =>
          !new File(s"$dir/celeborn-worker/shuffle_data/" +
            s"${sparkSession.sparkContext.applicationId}/$shuffleId").exists())
      })
      val deletedSuccessfullyString = shuffleIdShouldNotExist.map(shuffleId => {
        shuffleId.toString + ":" +
          workerDirs.map(dir =>
            !new File(s"$dir/celeborn-worker/shuffle_data/" +
              s"${sparkSession.sparkContext.applicationId}/$shuffleId").exists()).toList
      }).mkString(",")
      val createdSuccessfully = shuffleIdMustExist.forall(shuffleId => {
        workerDirs.exists(dir =>
          new File(s"$dir/celeborn-worker/shuffle_data/" +
            s"${sparkSession.sparkContext.applicationId}/$shuffleId").exists())
      })
      val createdSuccessfullyString = shuffleIdMustExist.map(shuffleId => {
        shuffleId.toString + ":" +
          workerDirs.map(dir =>
            new File(s"$dir/celeborn-worker/shuffle_data/" +
              s"${sparkSession.sparkContext.applicationId}/$shuffleId").exists()).toList
      }).mkString(",")
      println(s"${deletedSuccessfullyString} \t $createdSuccessfullyString")
      deletedSuccessfully && createdSuccessfully
    }

    override def run(): Unit = {
      var allDataInShape = checkDirStatus()
      while (!allDataInShape) {
        Thread.sleep(200)
        allDataInShape = checkDirStatus()
      }
    }
  }

  class CheckingThreadForStableStatus(
      shuffleIdShouldNotExist: Seq[Int],
      shuffleIdMustExist: Seq[Int],
      sparkSession: SparkSession)
    extends CheckingThread(shuffleIdShouldNotExist, shuffleIdMustExist, sparkSession) {
    override def run(): Unit = {
      val timeout = 20000
      var elapseTime = 0L
      var allDataInShape = checkDirStatus()
      while (!allDataInShape) {
        Thread.sleep(5000)
        println("init state not meet")
        allDataInShape = checkDirStatus()
      }
      while (allDataInShape) {
        Thread.sleep(5000)
        elapseTime += 5000
        if (elapseTime > timeout) {
          return
        }
        allDataInShape = checkDirStatus()
        if (!allDataInShape) {
          exception = new IllegalStateException("the directory state does not meet" +
            " the expected state")
          throw exception
        }
      }
    }
  }

  protected def triggerStorageCheckThread(
      shuffleIdShouldNotExist: Seq[Int],
      shuffleIdMustExist: Seq[Int],
      sparkSession: SparkSession,
      forStableStatusChecking: Boolean): CheckingThread = {
    val checkingThread =
      if (!forStableStatusChecking) {
        new CheckingThread(shuffleIdShouldNotExist, shuffleIdMustExist, sparkSession)
      } else {
        new CheckingThreadForStableStatus(shuffleIdShouldNotExist, shuffleIdMustExist, sparkSession)
      }
    checkingThread.setDaemon(true)
    checkingThread.start()
    checkingThread
  }

  protected def checkStorageValidation(thread: Thread, timeout: Long = 120 * 1000): Unit = {
    val checkingThread = thread.asInstanceOf[CheckingThread]
    checkingThread.join(timeout)
    if (checkingThread.isAlive || checkingThread.exception != null) {
      throw new IllegalStateException("the storage checking status failed," +
        s"${checkingThread.isAlive} ${if (checkingThread.exception != null) checkingThread.exception.getMessage
        else "NULL"}")
    }
  }

}
