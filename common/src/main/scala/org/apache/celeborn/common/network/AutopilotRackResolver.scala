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

import java.io.{BufferedReader, File, FileReader}
import java.util.prefs.Preferences
import java.util.regex.{Matcher, Pattern}

import org.apache.celeborn.common.internal.Logging

/**
 * Resolves the local machine's rack using Autopilot podset information.
 *
 * Reads machineinfo.csv to find the local machine's pod name, then parses
 * the pod name to extract cluster/spine/podset. The podset string
 * (format: CLUSTER-SPINE-PODSET, e.g. "BN1-04-015") is used as the rack value.
 */
object AutopilotRackResolver extends Logging {

  /** Standard Autopilot pod pattern: PODBN1SCH0401513 */
  private val AP_POD_REGEX: Pattern = Pattern.compile(
    "^POD(?<cluster>\\w{3,5}?)(\\w{3}|A{0,2})(?<spine>\\d{2})(?<podset>\\d{3})(?<pod>\\d{2})$")

  /** EAP host pattern: CH1BEAP00001893 */
  private val EAP_HOST_PATTERN: String =
    "(?<cluster>\\w{3,4})" +
      "(?<spine>EAP)" +
      "\\p{XDigit}{8}"

  /** EAP pod pattern: POD_PG/GUID/FD/digit */
  private val EAP_POD_PATTERN: String =
    "POD_PG/" +
      "(?<podset>.+)" +
      "/FD/" +
      "(?<pod>\\d+)"

  /** Combined EAP pattern: host/POD_PG/GUID/FD/digit */
  private val EAP_POD_REGEX: Pattern = Pattern.compile(
    "^" + EAP_HOST_PATTERN + "/" + EAP_POD_PATTERN)

  /** Physical host pattern: CH1PHY120081025 */
  private val PHYSICAL_NAME_REGEX: Pattern = Pattern.compile(
    "(?<cluster>\\w{3})PHY(?<spine>\\d{2})(?<podset>\\d{3})(?<pod>\\d{2})(?<port>\\d{2})")

  /** Default machineinfo.csv path per platform. */
  private val DEFAULT_MACHINE_INFO_FILE: String = {
    val os = System.getProperty("os.name", "").toLowerCase
    if (os.contains("win")) "D:/data/machineinfo.csv"
    else "/data/machineinfo.csv"
  }

  /**
   * Resolve the podset for the local machine by reading machineinfo.csv.
   * Returns the podset as a rack string (e.g. "/CLUSTER-SPINE-PODSET"),
   * or None if the information cannot be determined.
   */
  def resolveLocalPodset(): Option[String] = {
    val machineName = getLocalMachineName()
    if (machineName == null || machineName.isEmpty) {
      logWarning("Cannot determine local machine name from environment")
      return None
    }
    logInfo(s"Local machine name: $machineName")

    val fields = lookupCsvFields(machineName, DEFAULT_MACHINE_INFO_FILE,
      Seq("PodName", "PhysicalMachineName"))
    val podName = fields.getOrElse("PodName", null)
    if (podName == null || podName.isEmpty) {
      logWarning(s"Cannot find pod name for machine $machineName " +
        s"in $DEFAULT_MACHINE_INFO_FILE")
      return None
    }
    logInfo(s"Pod name for $machineName: $podName")

    val physicalName = fields.getOrElse("PhysicalMachineName", null)

    // For EAP VMs, try reading the real physical host from Hyper-V registry
    val physicalHostVM = getPhysicalHostVM()
    if (physicalHostVM != null) {
      logInfo(s"Physical host VM from Hyper-V registry: $physicalHostVM")
    }

    val podset = extractPodset(podName, physicalName, physicalHostVM)
    if (podset == null) {
      logWarning(s"Cannot parse podset from pod name: $podName")
      return None
    }
    logInfo(s"Resolved podset: $podset")
    Some("/" + podset)
  }

  /** Get the local machine name from environment variables. */
  private[network] def getLocalMachineName(): String = {
    val os = System.getProperty("os.name", "").toLowerCase
    if (os.contains("win")) {
      System.getenv("COMPUTERNAME")
    } else {
      System.getenv("AP_MACHINE_NAME")
    }
  }

  /**
   * Read machineinfo.csv once and return the values of the requested fields
   * for the row matching the machine name.
   */
  private[network] def lookupCsvFields(
      machineName: String,
      csvPath: String,
      fieldNames: Seq[String]): Map[String, String] = {
    val file = new File(csvPath)
    if (!file.exists() || !file.canRead) {
      logWarning(s"machineinfo.csv not found or not readable: $csvPath")
      return Map.empty
    }

    var reader: BufferedReader = null
    try {
      reader = new BufferedReader(new FileReader(file))

      // Parse header — first non-comment line starting with "#Fields:"
      var headerLine: String = null
      var line = reader.readLine()
      while (line != null && headerLine == null) {
        val trimmed = line.trim
        if (trimmed.startsWith("#Fields:")) {
          headerLine = trimmed.substring("#Fields:".length).trim
        }
        line = reader.readLine()
      }
      if (headerLine == null) {
        logWarning(s"Cannot find #Fields: header in $csvPath")
        return Map.empty
      }

      val headers = headerLine.split(",").map(_.trim)
      val machineNameIdx = headers.indexOf("MachineName")
      if (machineNameIdx < 0) {
        logWarning(s"Missing MachineName column in $csvPath")
        return Map.empty
      }
      val fieldIndices = fieldNames.map(name => name -> headers.indexOf(name)).toMap
      val missingFields = fieldIndices.filter(_._2 < 0).keys
      if (missingFields.nonEmpty) {
        logWarning(s"Missing columns ${missingFields.mkString(", ")} in $csvPath")
      }

      // line already points to the first data row after header scan
      while (line != null) {
        val trimmed = line.trim
        if (!trimmed.startsWith("#") && trimmed.nonEmpty) {
          val fields = trimmed.split(",", -1)
          if (fields.length > machineNameIdx &&
              fields(machineNameIdx).equalsIgnoreCase(machineName)) {
            return fieldIndices.collect {
              case (name, idx) if idx >= 0 && idx < fields.length => name -> fields(idx)
            }
          }
        }
        line = reader.readLine()
      }

      logWarning(s"Machine $machineName not found in $csvPath")
      Map.empty
    } catch {
      case e: Exception =>
        logError(s"Error reading $csvPath", e)
        Map.empty
    } finally {
      if (reader != null) {
        try { reader.close() } catch { case _: Exception => }
      }
    }
  }

  /**
   * Extract the podset from a pod name string.
   * Returns "CLUSTER-SPINE-PODSET" (e.g. "BN1-04-015") or null.
   *
   * Tries in order:
   * 1. EAP pods with POD_PG/ prefix (combined with physicalName)
   * 2. Standard Autopilot pods (e.g. PODBN1SCH0401513)
   * 3. Hyper-V physical host fallback (e.g. CH1PHY120081025 from registry)
   * If none match, returns null (unknown rack)
   */
  private[network] def extractPodset(
      podName: String,
      physicalName: String,
      physicalHostVM: String = null): String = {
    if (podName == null || podName.isEmpty) return null

    if (podName.startsWith("POD_PG/") && physicalName != null) {
      // EAP-style: combine physicalName + "/" + podName for the regex
      val combined = physicalName + "/" + podName
      val matcher = EAP_POD_REGEX.matcher(combined)
      if (matcher.find(0)) {
        val cluster = matcher.group("cluster")
        val spine = matcher.group("spine")
        val podset = matcher.group("podset")
        return s"$cluster-$spine-$podset"
      }
    }

    // Standard Autopilot pod
    val matcher = AP_POD_REGEX.matcher(podName)
    if (matcher.find(0)) {
      val cluster = matcher.group("cluster")
      val spine = matcher.group("spine")
      val podset = matcher.group("podset")
      return s"$cluster-$spine-$podset"
    }

    // Hyper-V physical host fallback (EAP VMs report the real physical host
    // via the Windows registry, e.g. CH1PHY120081025)
    if (physicalHostVM != null) {
      val phyMatcher = PHYSICAL_NAME_REGEX.matcher(physicalHostVM)
      if (phyMatcher.find(0)) {
        val cluster = phyMatcher.group("cluster")
        val spine = phyMatcher.group("spine")
        val podset = phyMatcher.group("podset")
        return s"$cluster-$spine-$podset"
      }
    }

    null
  }

  /**
   * Read the Physical Host Name from the Hyper-V registry key.
   * On EAP VMs, the real physical host (e.g. CH1PHY120081025) is available at:
   * HKLM\SOFTWARE\Microsoft\Virtual Machine\Guest\Parameters\PhysicalHostName
   */
  private[network] def getPhysicalHostVM(): String = {
    try {
      val systemRoot = Preferences.systemRoot()
      val regOpenKey = systemRoot.getClass.getDeclaredMethod(
        "WindowsRegOpenKey", classOf[Int], classOf[Array[Byte]], classOf[Int])
      regOpenKey.setAccessible(true)
      val regCloseKey = systemRoot.getClass.getDeclaredMethod(
        "WindowsRegCloseKey", classOf[Int])
      regCloseKey.setAccessible(true)
      val regQueryValueEx = systemRoot.getClass.getDeclaredMethod(
        "WindowsRegQueryValueEx", classOf[Int], classOf[Array[Byte]])
      regQueryValueEx.setAccessible(true)

      val key = "SOFTWARE\\Microsoft\\Virtual Machine\\Guest\\Parameters"
      val HKEY_LOCAL_MACHINE = 0x80000002
      val KEY_READ = 0x20019

      val handles = regOpenKey.invoke(systemRoot,
        Integer.valueOf(HKEY_LOCAL_MACHINE),
        toCstr(key),
        Integer.valueOf(KEY_READ)).asInstanceOf[Array[Int]]
      if (handles(1) != 0) return null // REG_SUCCESS = 0

      val valb = regQueryValueEx.invoke(systemRoot,
        Integer.valueOf(handles(0)),
        toCstr("PhysicalHostName")).asInstanceOf[Array[Byte]]
      regCloseKey.invoke(systemRoot, Integer.valueOf(handles(0)))

      if (valb != null) new String(valb).trim.stripSuffix("\u0000")
      else null
    } catch {
      case e: Exception =>
        logDebug(s"Cannot read Hyper-V registry: ${e.getMessage}")
        null
    }
  }

  private def toCstr(str: String): Array[Byte] = {
    val bytes = new Array[Byte](str.length + 1)
    var i = 0
    while (i < str.length) {
      bytes(i) = str.charAt(i).toByte
      i += 1
    }
    bytes(i) = 0
    bytes
  }
}
