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

package org.apache.celeborn.common.network

import java.io.{File, FileWriter}

import org.scalatest.funsuite.AnyFunSuite

class AutopilotRackResolverSuite extends AnyFunSuite {

  // Realistic machineinfo.csv header (without StaticIP)
  private val CSV_HEADER =
    "#Fields:MachineName,PodName,MachineFunction,Port,Image,SKU," +
      "ScaleUnit,Cluster,NodeId,IncarnationId,MachineGuid,PhysicalMachineName"

  private def createCsvFile(rows: String*): File = {
    val tmpFile = File.createTempFile("machineinfo", ".csv")
    tmpFile.deleteOnExit()
    val writer = new FileWriter(tmpFile)
    writer.write(CSV_HEADER + "\n")
    rows.foreach { row =>
      writer.write(row + "\n")
    }
    writer.flush()
    writer.close()
    tmpFile
  }

  // --- End-to-end: CSV lookup → extractPodset ---

  test("EAP machine CHIEEEAP000DF09 resolves to CH2-10-128") {
    val csvFile = createCsvFile(
      "CHIEEEAP000DF09,PODCH2AA1012816,YarnNM1,0,retail.amd64,AzureSSVM_AZAP,101281,CH2,1001,42,guid1,CHIEEEAP000DF09")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "CHIEEEAP000DF09", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "CH2-10-128")
  }

  test("EAP machine CHIEEEAP000DF0A resolves to CH2-10-126") {
    val csvFile = createCsvFile(
      "CHIEEEAP000DF0A,PODCH2AA1012607,YarnNM1,0,retail.amd64,AzureSSVM_AZAP,101260,CH2,1002,43,guid2,CHIEEEAP000DF0A")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "CHIEEEAP000DF0A", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "CH2-10-126")
  }

  test("EAP machine CHIEEEAP000DF0B resolves to CH2-10-141") {
    val csvFile = createCsvFile(
      "CHIEEEAP000DF0B,PODCH2AA1014108,YarnNM1,0,retail.amd64,AzureSSVM_AZAP,101410,CH2,1003,44,guid3,CHIEEEAP000DF0B")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "CHIEEEAP000DF0B", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "CH2-10-141")
  }

  test("Standard AP machine with SCH pod resolves correctly") {
    val csvFile = createCsvFile(
      "BN1MACHINE01,PODBN1SCH0401513,Worker,0,base.amd64,Standard_D8,040151,BN1,2001,10,guid4,BN1PHY0401513")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "BN1MACHINE01", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "BN1-04-015")
  }

  test("Standard AP machine with 5-char cluster resolves correctly") {
    val csvFile = createCsvFile(
      "MWH04NODE01,PODMWH04SCH0100107,Worker,0,base.amd64,Standard_D8,010010,MWH04,3001,11,guid5,MWH04PHY0100107")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "MWH04NODE01", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "MWH04-01-001")
  }

  test("Multiple machines in CSV, lookup finds correct one") {
    val csvFile = createCsvFile(
      "CHIEEEAP000DF09,PODCH2AA1012816,YarnNM1,0,retail.amd64,AzureSSVM_AZAP,101281,CH2,1001,42,guid1,CHIEEEAP000DF09",
      "CHIEEEAP000DF0A,PODCH2AA1012607,YarnNM1,0,retail.amd64,AzureSSVM_AZAP,101260,CH2,1002,43,guid2,CHIEEEAP000DF0A",
      "BN1MACHINE01,PODBN1SCH0401513,Worker,0,base.amd64,Standard_D8,040151,BN1,2001,10,guid4,BN1PHY0401513")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "CHIEEEAP000DF0A", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "CH2-10-126")
  }

  // --- extractPodset unit tests ---

  test("VMSS-style pod PODCHI221063012 resolves to CHI22-10-630") {
    val result = AutopilotRackResolver.extractPodset("PODCHI221063012", null)
    assert(result === "CHI22-10-630")
  }

  test("VMSS-style pod PODCHI221051009 resolves to CHI22-10-510") {
    val result = AutopilotRackResolver.extractPodset("PODCHI221051009", null)
    assert(result === "CHI22-10-510")
  }

  test("VMSS EAP machine CHIEEEAP0029FE6 end-to-end resolves to CHI22-10-630") {
    val csvFile = createCsvFile(
      "CHIEEEAP0029FE6,PODCHI221063012,YarnNM,0,retail.amd64,AzureSSVM_Internal,2210630,CHI22,1001,42,guid1,CHIEEEAP0029FE6")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "CHIEEEAP0029FE6", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "CHI22-10-630")
  }

  test("VMSS EAP machine CHIEEEAP0029FE7 end-to-end resolves to CHI22-10-510") {
    val csvFile = createCsvFile(
      "CHIEEEAP0029FE7,PODCHI221051009,YarnNM,0,retail.amd64,AzureSSVM_Internal,2210510,CHI22,1002,43,guid2,CHIEEEAP0029FE7")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "CHIEEEAP0029FE7", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "CHI22-10-510")
  }

  test("VMSS-style pod PODLVL031090712 resolves to LVL03-10-907") {
    val result = AutopilotRackResolver.extractPodset("PODLVL031090712", null)
    assert(result === "LVL03-10-907")
  }

  test("VMSS EAP machine BNZEEAP00025C7C end-to-end resolves to LVL03-10-907") {
    val csvFile = createCsvFile(
      "BNZEEAP00025C7C,PODLVL031090712,YarnNM,0,retail.amd64,AzureSSVM_Internal_BingBalancedGen7_128iad_2vNuma,3109072,BNZ,2001,10,guid6,BNZEEAP00025C7C")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "BNZEEAP00025C7C", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "LVL03-10-907")
  }

  test("VMSS EAP machine BNZEEAP00025C7D end-to-end resolves to LVL03-10-907") {
    val csvFile = createCsvFile(
      "BNZEEAP00025C7D,PODLVL031090703,YarnNM,0,retail.amd64,AzureSSVM_Internal_BingBalancedGen7_128iad_2vNuma,3109073,BNZ,2002,11,guid7,BNZEEAP00025C7D")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "BNZEEAP00025C7D", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "LVL03-10-907")
  }

  test("VMSS EAP machine MWHEEEAP0020FA3 end-to-end resolves to MWH05-11-106") {
    val csvFile = createCsvFile(
      "MWHEEEAP0020FA3,PODMWH051110608,YarnNM1,0,retail.amd64,AzureSSVM_AZAP,5111060,MWH05,3001,12,guid8,MWHEEEAP0020FA3")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "MWHEEEAP0020FA3", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "MWH05-11-106")
  }

  test("VMSS EAP machine MWHEEEAP0020FA4 end-to-end resolves to MWH05-10-504") {
    val csvFile = createCsvFile(
      "MWHEEEAP0020FA4,PODMWH051050405,YarnNM1,0,retail.amd64,AzureSSVM_AZAP,5105040,MWH05,3002,13,guid9,MWHEEEAP0020FA4")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "MWHEEEAP0020FA4", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "MWH05-10-504")
  }

  test("VMSS EAP machine MWHEEEAP0020FA5 end-to-end resolves to MWH05-10-401") {
    val csvFile = createCsvFile(
      "MWHEEEAP0020FA5,PODMWH051040108,YarnNM1,0,retail.amd64,AzureSSVM_AZAP,5104013,MWH05,3003,14,guid10,MWHEEEAP0020FA5")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "MWHEEEAP0020FA5", csvFile.getAbsolutePath, Seq("PodName", "PhysicalMachineName"))
    val podset = AutopilotRackResolver.extractPodset(
      fields("PodName"), fields("PhysicalMachineName"))
    assert(podset === "MWH05-10-401")
  }

  test("EAP pod with POD_PG prefix") {
    val podName = "POD_PG/98856e90-f2ff-408c-90e2-6134842c3f72/FD/4"
    val physicalName = "CH1BEAP00001893"
    val result = AutopilotRackResolver.extractPodset(podName, physicalName)
    assert(result === "CH1B-EAP-98856e90-f2ff-408c-90e2-6134842c3f72")
  }

  test("Hyper-V physical host fallback: CH1PHY120081025") {
    val result = AutopilotRackResolver.extractPodset(
      "SOME_UNKNOWN_POD", null, "CH1PHY120081025")
    assert(result === "CH1-12-008")
  }

  test("Null pod name returns null") {
    assert(AutopilotRackResolver.extractPodset(null, null) === null)
  }

  test("Empty pod name returns null") {
    assert(AutopilotRackResolver.extractPodset("", null) === null)
  }

  test("Unrecognized pod name with no fallback returns null") {
    assert(AutopilotRackResolver.extractPodset("RANDOM_STRING", null) === null)
  }

  // --- lookupCsvFields edge cases ---

  test("lookupCsvFields is case-insensitive for machine name") {
    val csvFile = createCsvFile(
      "CHIEEEAP000DF09,PODCH2AA1012816,YarnNM1,0,retail.amd64,AzureSSVM_AZAP,101281,CH2,1001,42,guid1,CHIEEEAP000DF09")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "chieeeap000df09", csvFile.getAbsolutePath, Seq("PodName"))
    assert(fields("PodName") === "PODCH2AA1012816")
  }

  test("lookupCsvFields returns empty map for missing machine") {
    val csvFile = createCsvFile(
      "CHIEEEAP000DF09,PODCH2AA1012816,YarnNM1,0,retail.amd64,AzureSSVM_AZAP,101281,CH2,1001,42,guid1,CHIEEEAP000DF09")
    val fields = AutopilotRackResolver.lookupCsvFields(
      "NOTFOUND", csvFile.getAbsolutePath, Seq("PodName"))
    assert(fields.isEmpty)
  }

  test("lookupCsvFields returns empty map for nonexistent file") {
    val fields = AutopilotRackResolver.lookupCsvFields(
      "MYMACHINE", "/nonexistent/path/machineinfo.csv", Seq("PodName"))
    assert(fields.isEmpty)
  }

  test("lookupCsvFields skips comment lines in data") {
    val tmpFile = File.createTempFile("machineinfo", ".csv")
    tmpFile.deleteOnExit()
    val writer = new FileWriter(tmpFile)
    writer.write(CSV_HEADER + "\n")
    writer.write("# This is a comment\n")
    writer.write("MYMACHINE,PODBN1SCH0401513,Worker,0,base.amd64,Standard_D8,040151,BN1,2001,10,guid4,BN1PHY0401513\n")
    writer.flush()
    writer.close()

    val fields = AutopilotRackResolver.lookupCsvFields(
      "MYMACHINE", tmpFile.getAbsolutePath, Seq("PodName"))
    assert(fields("PodName") === "PODBN1SCH0401513")
  }
}
