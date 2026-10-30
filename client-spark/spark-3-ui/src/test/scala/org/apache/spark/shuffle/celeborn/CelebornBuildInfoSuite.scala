/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.spark.shuffle.celeborn

import org.apache.spark.util.kvstore.InMemoryStore
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import org.apache.celeborn.ProjectConstants

@RunWith(classOf[JUnit4])
class CelebornBuildInfoSuite {

  @Test
  def buildInfoRoundTrip(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)

    // Before the plugin persists anything, the UI falls back to empty build info.
    assertTrue(statusStore.buildInfo().info.isEmpty)

    // Mirrors what CelebornListener.onOtherEvent persists.
    store.write(new CelebornBuildInfoUIData(Seq(
      "Version" -> ProjectConstants.VERSION,
      "Commit Id" -> ProjectConstants.getGitCommitId,
      "Revision" -> ProjectConstants.REVISION)))

    val info = statusStore.buildInfo().info
    val infoMap = info.toMap
    assertEquals(3, info.size)
    assertEquals(ProjectConstants.VERSION, infoMap("Version"))
    assertEquals(ProjectConstants.REVISION, infoMap("Revision"))
  }

  @Test
  def commitIdIsShortRevision(): Unit = {
    val expected =
      if (ProjectConstants.REVISION.length >= 8) ProjectConstants.REVISION.substring(0, 8)
      else ProjectConstants.REVISION
    assertEquals(expected, ProjectConstants.getGitCommitId)
  }

  @Test
  def projectConstantsArePopulated(): Unit = {
    assertFalse(ProjectConstants.VERSION.isEmpty)
    assertFalse(ProjectConstants.REVISION.isEmpty)
  }
}
