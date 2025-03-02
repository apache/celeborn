package org.apache.celeborn.common.metrics.source

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class JVMCPUSourceSuite extends CelebornFunSuite {
  test("Test CPU load") {
    val conf = new CelebornConf()

    val source = new JVMCPUSource(conf, Role.WORKER)
    val res = source.getMetrics
    assert(res.contains("JVMCPULoad_Value"))
  }
}
