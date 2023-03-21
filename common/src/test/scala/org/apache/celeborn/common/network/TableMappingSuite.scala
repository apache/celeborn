package org.apache.celeborn.common.network

import java.io.File
import java.util

import org.apache.hadoop.shaded.com.google.common.base.Charsets
import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.hadoop.shaded.com.google.common.io.Files
import org.junit.Assert

class TableMappingSuite extends CelebornFunSuite{
  private val hostName1: String = "1.2.3.4"
  private val hostName2: String = "5.6.7.8"

  test("testResolve") {
    val mapFile: File = File.createTempFile(getClass.getSimpleName + ".testResolve", ".txt")
    Files.asCharSink(mapFile, Charsets.UTF_8).write(hostName1 + " /rack1\n" + hostName2 + "\t/rack2\n")
    mapFile.deleteOnExit()
    val mapping: TableMapping = new TableMapping
    val conf: CelebornConf = new CelebornConf
    conf.set(CelebornConf.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, mapFile.getCanonicalPath)
    mapping.setConf(conf)
    val names: java.util.List[String] = new java.util.ArrayList[String]
    names.add(hostName1)
    names.add(hostName2)
    val result: java.util.List[String] = mapping.resolve(names)
    Assert.assertEquals(names.size, result.size)
    Assert.assertEquals("/rack1", result.get(0))
    Assert.assertEquals("/rack2", result.get(1))
  }

  test("testTableCaching") {
    val mapFile: File = File.createTempFile(getClass.getSimpleName + ".testTableCaching", ".txt")
    Files.asCharSink(mapFile, Charsets.UTF_8).write(hostName1 + " /rack1\n" + hostName2 + "\t/rack2\n")
    mapFile.deleteOnExit()
    val mapping: TableMapping = new TableMapping
    val conf: CelebornConf = new CelebornConf()
    conf.set(CelebornConf.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, mapFile.getCanonicalPath)
    mapping.setConf(conf)
    val names: java.util.List[String] = new java.util.ArrayList[String]
    names.add(hostName1)
    names.add(hostName2)
    val result1: java.util.List[String] = mapping.resolve(names)
    Assert.assertEquals(names.size, result1.size)
    Assert.assertEquals("/rack1", result1.get(0))
    Assert.assertEquals("/rack2", result1.get(1))
    // unset the file, see if it gets read again
    conf.set(CelebornConf.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, "some bad value for a file")
    val result2: java.util.List[String] = mapping.resolve(names)
    Assert.assertEquals(result1, result2)
  }

 test("testNoFile") {
    val mapping: TableMapping = new TableMapping
    val conf: CelebornConf = new CelebornConf()
    mapping.setConf(conf)
    val names: java.util.List[String] = new java.util.ArrayList[String]
    names.add(hostName1)
    names.add(hostName2)
    val result: java.util.List[String] = mapping.resolve(names)
    Assert.assertEquals(names.size, result.size)
    Assert.assertEquals(NetworkTopology.DEFAULT_RACK, result.get(0))
    Assert.assertEquals(NetworkTopology.DEFAULT_RACK, result.get(1))
  }
  test("testFileDoesNotExist") {
    val mapping: TableMapping = new TableMapping
    val conf: CelebornConf = new CelebornConf()
    conf.set(CelebornConf.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, "/this/file/does/not/exist")
    mapping.setConf(conf)
    val names: java.util.List[String] = new java.util.ArrayList[String]
    names.add(hostName1)
    names.add(hostName2)
    val result: java.util.List[String] = mapping.resolve(names)
    Assert.assertEquals(names.size, result.size)
    Assert.assertEquals(result.get(0), NetworkTopology.DEFAULT_RACK)
    Assert.assertEquals(result.get(1), NetworkTopology.DEFAULT_RACK)
  }

  test("testClearingCachedMappings") {
    val mapFile: File = File.createTempFile(getClass.getSimpleName + ".testClearingCachedMappings", ".txt")
    Files.asCharSink(mapFile, Charsets.UTF_8).write(hostName1 + " /rack1\n" + hostName2 + "\t/rack2\n")
    mapFile.deleteOnExit()
    val mapping: TableMapping = new TableMapping
    val conf: CelebornConf = new CelebornConf()
    conf.set(CelebornConf.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, mapFile.getCanonicalPath)
    mapping.setConf(conf)
    var names: java.util.List[String] = new java.util.ArrayList[String]
    names.add(hostName1)
    names.add(hostName2)
    var result: java.util.List[String] = mapping.resolve(names)
    Assert.assertEquals(names.size, result.size)
    Assert.assertEquals("/rack1", result.get(0))
    Assert.assertEquals("/rack2", result.get(1))
    Files.asCharSink(mapFile, Charsets.UTF_8).write("")
    mapping.reloadCachedMappings()
    names = new java.util.ArrayList[String]
    names.add(hostName1)
    names.add(hostName2)
    result = mapping.resolve(names)
    Assert.assertEquals(names.size, result.size)
    Assert.assertEquals(NetworkTopology.DEFAULT_RACK, result.get(0))
    Assert.assertEquals(NetworkTopology.DEFAULT_RACK, result.get(1))
  }
//
//
//  @Test(timeout = 60000)
//  @throws[IOException]
//  def testBadFile(): Unit = {
//    val mapFile: File = File.createTempFile(getClass.getSimpleName + ".testBadFile", ".txt")
//    Files.asCharSink(mapFile, Charsets.UTF_8).write("bad contents")
//    mapFile.deleteOnExit()
//    val mapping: TableMapping = new TableMapping
//    val conf: Configuration = new Configuration
//    conf.set(NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, mapFile.getCanonicalPath)
//    mapping.setConf(conf)
//    val names: List[String] = new ArrayList[String]
//    names.add(hostName1)
//    names.add(hostName2)
//    val result: List[String] = mapping.resolve(names)
//    assertEquals(names.size, result.size)
//    assertEquals(result.get(0), NetworkTopology.DEFAULT_RACK)
//    assertEquals(result.get(1), NetworkTopology.DEFAULT_RACK)
//  }
}
