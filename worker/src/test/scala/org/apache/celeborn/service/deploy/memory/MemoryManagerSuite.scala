package org.apache.celeborn.service.deploy.memory

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE, WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager
class MemoryManagerSuite extends AnyFunSuite with BeforeAndAfterEach {

  // reset the memory manager before each test
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    MemoryManager.reset()
  }

  test("Init MemoryManager with invalid configuration") {
    val conf = new CelebornConf().set(WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE, 0.95)
      .set(WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE, 0.85)
    val caught =
      intercept[IllegalArgumentException] {
        MemoryManager.initialize(conf);
      }
    assert(
      caught.getMessage == s"Invalid config, ${WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE.key}(${0.85}) " +
        s"should be greater than ${WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE.key}(${0.95})")
  }
}
