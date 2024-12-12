package org.apache.celeborn.tests.tez

class BroadcastAndOneToOneTest extends TezIntegrationTestBase {

  val outputPath = "broadcast_oneone_output"

  test("celeborn tez integration test - Ordered Word Count") {
    run()
  }

  override def getTestTool = new BroadcastAndOneToOneExample

  override def getTestArgs(uniqueOutputName: String) = new Array[String](0)

  override def getOutputDir(uniqueOutputName: String): String = outputPath + "/" + uniqueOutputName

  override def verifyResults(originPath: String, rssPath: String): Unit = {

  }

}
