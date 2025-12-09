package org.apache.celeborn.tests.spark

import org.apache.celeborn.common.internal.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}

import java.nio.file.Files

trait MiniDfsClusterFeature extends Logging {
  private var cluster: MiniDFSCluster = null
  private var conf: HdfsConfiguration = null
  private var hdfsDir: String = "/shuffle-test"

  def getHDFSConfigs(): Map[String, String] = {
    Map(
      "celeborn.storage.hdfs.dir" -> "hdfs://localhost/shuffle-test"
    )
  }

  private def createHdfsTempDir(): String = {
    val tmpDir = Files.createTempDirectory("hdfs-")
    logInfo(s"create hdfs temp dir: $tmpDir")
    tmpDir.toFile.deleteOnExit()
    tmpDir.toAbsolutePath.toString
  }

  def HdfsClusterSetupAtBeginning(): Unit = {
    conf = new HdfsConfiguration()

    FileSystem.setDefaultUri(conf, "hdfs://localhost")
    val baseDir = createHdfsTempDir()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir)

    cluster = new MiniDFSCluster.Builder(conf).nameNodePort(8020).numDataNodes(3).build()

    val fs = FileSystem.get(conf)
    val p = new Path(hdfsDir)
    fs.mkdirs(p)
  }

  def HdfsClusterShutdownAtEnd(): Unit = {
    if (cluster != null) cluster.shutdown()
  }
}
