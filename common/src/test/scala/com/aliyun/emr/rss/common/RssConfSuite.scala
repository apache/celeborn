package com.aliyun.emr.rss.common

import com.aliyun.emr.RssFunSuite
import com.aliyun.emr.rss.common.RssConf.{haMasterHosts, masterPort}

class RssConfSuite extends RssFunSuite{

  test("issue 26: rss.master.address support multi host") {
    val conf = new RssConf()
    conf.set("rss.master.address", "localhost1:9097,localhost2:9097")
    assert("localhost1,localhost2" == haMasterHosts(conf))
    assert(9097 == masterPort(conf))
  }
}
