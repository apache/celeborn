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

private[tests] object StorageCheckUtils {

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
      println(s"shuffle-to-be-deleted status: $deletedSuccessfullyString \n" +
        s"shuffle-to-be-created status: $createdSuccessfullyString")
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

  def triggerStorageCheckThread(
      workerDirs: Seq[String],
      shuffleIdShouldNotExist: Seq[Int],
      shuffleIdMustExist: Seq[Int],
      sparkSession: SparkSession): CheckingThread = {
    val checkingThread =
      new CheckingThread(workerDirs, shuffleIdShouldNotExist, shuffleIdMustExist, sparkSession)
    checkingThread.setDaemon(true)
    checkingThread.start()
    checkingThread
  }

  def checkStorageValidation(thread: Thread, timeout: Long = 1200 * 1000): Unit = {
    val checkingThread = thread.asInstanceOf[CheckingThread]
    checkingThread.join(timeout)
    if (checkingThread.isAlive || checkingThread.exception != null) {
      throw new IllegalStateException("the storage checking status failed," +
        s"${checkingThread.isAlive} ${if (checkingThread.exception != null) checkingThread.exception.getMessage
        else "NULL"}")
    }
  }
}
