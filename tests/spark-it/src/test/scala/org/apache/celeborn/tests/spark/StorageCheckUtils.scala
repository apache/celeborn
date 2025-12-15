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

package org.apache.celeborn.tests.spark

import java.io.File

import org.apache.spark.sql.SparkSession

object StorageCheckUtils {

  class CheckingThread(
                        workerDirs: Seq[String],
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
      val createdSuccessfully = shuffleIdMustExist.forall(shuffleId => {
        workerDirs.exists(dir =>
          new File(s"$dir/celeborn-worker/shuffle_data/" +
            s"${sparkSession.sparkContext.applicationId}/$shuffleId").exists())
      })
      println(deletedSuccessfully + ":" + createdSuccessfully)
      deletedSuccessfully && createdSuccessfully
    }

    override def run(): Unit = {
      var allDataInShape = checkDirStatus()
      while (!allDataInShape) {
        Thread.sleep(1000)
        allDataInShape = checkDirStatus()
      }
    }
  }

  class CheckingThreadForStableStatus(
                                       workerDirs: Seq[String],
                                       shuffleIdShouldNotExist: Seq[Int],
                                       shuffleIdMustExist: Seq[Int],
                                       sparkSession: SparkSession)
    extends CheckingThread(workerDirs, shuffleIdShouldNotExist, shuffleIdMustExist, sparkSession) {

    override def run(): Unit = {
      val timeout = 60000
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

  def triggerStorageCheckThread(
                                 workerDirs: Seq[String],
                                 shuffleIdShouldNotExist: Seq[Int],
                                 shuffleIdMustExist: Seq[Int],
                                 sparkSession: SparkSession,
                                 forStableStatusChecking: Boolean): CheckingThread = {
    val checkingThread =
      if (!forStableStatusChecking) {
        new CheckingThread(workerDirs, shuffleIdShouldNotExist, shuffleIdMustExist, sparkSession)
      } else {
        new CheckingThreadForStableStatus(
          workerDirs,
          shuffleIdShouldNotExist,
          shuffleIdMustExist,
          sparkSession)
      }
    checkingThread.setDaemon(true)
    checkingThread.start()
    checkingThread
  }

  def checkStorageValidation(checkingThread: Thread): Unit = {
    checkingThread.join(120 * 1000)
    if (checkingThread.isAlive || checkingThread.asInstanceOf[CheckingThread].exception != null) {
      checkingThread.interrupt()
      throw new IllegalStateException("the storage checking status failed")
    }
  }

}
