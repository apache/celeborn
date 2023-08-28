package org.apache.celeborn.service.deploy.worker.storage

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{WORKER_GRACEFUL_SHUTDOWN_ENABLED, WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH}
import org.apache.celeborn.service.deploy.worker.WorkerSource

class StorageManagerSuite extends CelebornFunSuite {

  test("[CELEBORN-926] saveAllCommittedFileInfosToDB cause IllegalMonitorStateException") {

    val conf = new CelebornConf().set(WORKER_GRACEFUL_SHUTDOWN_ENABLED, true).set(
      WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH,
      "/tmp/recover")
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    // should not throw IllegalMonitorStateException exception
    storageManager.saveAllCommittedFileInfosToDB()
  }
}
