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

package org.apache.celeborn.plugin.flink;

import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.plugin.flink.config.PluginConf;

public class PluginConfSuiteJ {
  @Test
  public void testColesce() {
    Configuration flinkConf = new Configuration();
    CelebornConf celebornConf = new CelebornConf();
    Assert.assertEquals(
        "8m", PluginConf.colesce(flinkConf, celebornConf, PluginConf.MIN_MEMORY_PER_PARTITION));
    Assert.assertEquals(
        "8m", PluginConf.colesce(flinkConf, celebornConf, PluginConf.MIN_MEMORY_PER_GATE));
    Assert.assertTrue(
        Integer.MAX_VALUE
            == Integer.valueOf(
                PluginConf.colesce(flinkConf, celebornConf, PluginConf.NUM_CONCURRENT_READINGS)));
    Assert.assertEquals(
        "64m", PluginConf.colesce(flinkConf, celebornConf, PluginConf.MEMORY_PER_RESULT_PARTITION));
    Assert.assertEquals(
        "32m", PluginConf.colesce(flinkConf, celebornConf, PluginConf.MEMORY_PER_INPUT_GATE));

    Assert.assertEquals(
        "true", PluginConf.colesce(flinkConf, celebornConf, PluginConf.ENABLE_DATA_COMPRESSION));
    Assert.assertEquals(
        "LZ4",
        PluginConf.colesce(flinkConf, celebornConf, PluginConf.REMOTE_SHUFFLE_COMPRESSION_CODEC));

    flinkConf.setString(PluginConf.MIN_MEMORY_PER_PARTITION.name, "16m");
    flinkConf.setString(PluginConf.MIN_MEMORY_PER_GATE.name, "17m");
    flinkConf.setString(PluginConf.NUM_CONCURRENT_READINGS.name, "12323");
    flinkConf.setString(PluginConf.MEMORY_PER_RESULT_PARTITION.name, "1888m");
    flinkConf.setString(PluginConf.MEMORY_PER_INPUT_GATE.name, "176m");
    flinkConf.setString(PluginConf.ENABLE_DATA_COMPRESSION.name, "false");
    flinkConf.setString(PluginConf.REMOTE_SHUFFLE_COMPRESSION_CODEC.name, "lz423");
    Assert.assertEquals(
        "16m", PluginConf.colesce(flinkConf, celebornConf, PluginConf.MIN_MEMORY_PER_PARTITION));
    Assert.assertEquals(
        "17m", PluginConf.colesce(flinkConf, celebornConf, PluginConf.MIN_MEMORY_PER_GATE));
    Assert.assertTrue(
        12323
            == Integer.valueOf(
                PluginConf.colesce(flinkConf, celebornConf, PluginConf.NUM_CONCURRENT_READINGS)));
    Assert.assertEquals(
        "1888m",
        PluginConf.colesce(flinkConf, celebornConf, PluginConf.MEMORY_PER_RESULT_PARTITION));
    Assert.assertEquals(
        "176m", PluginConf.colesce(flinkConf, celebornConf, PluginConf.MEMORY_PER_INPUT_GATE));
    Assert.assertEquals(
        "false", PluginConf.colesce(flinkConf, celebornConf, PluginConf.ENABLE_DATA_COMPRESSION));
    Assert.assertEquals(
        "lz423",
        PluginConf.colesce(flinkConf, celebornConf, PluginConf.REMOTE_SHUFFLE_COMPRESSION_CODEC));
  }

  @Test
  public void testColesceCeleborn() {
    Configuration flinkConf = new Configuration();
    CelebornConf celebornConf = new CelebornConf();
    celebornConf.set(PluginConf.MEMORY_PER_RESULT_PARTITION.alterName, "128m");
    celebornConf.set(PluginConf.REMOTE_SHUFFLE_COMPRESSION_CODEC.alterName, "ZSTD");
    Assert.assertEquals(
        "128m",
        PluginConf.colesce(flinkConf, celebornConf, PluginConf.MEMORY_PER_RESULT_PARTITION));
    Assert.assertEquals(
        "ZSTD",
        PluginConf.colesce(flinkConf, celebornConf, PluginConf.REMOTE_SHUFFLE_COMPRESSION_CODEC));
  }

  @Test
  public void testColesceFlinkCeleborn() {
    Configuration flinkConf = new Configuration();
    flinkConf.setString(PluginConf.MEMORY_PER_RESULT_PARTITION.name, "78m");
    flinkConf.setString(PluginConf.REMOTE_SHUFFLE_COMPRESSION_CODEC.name, "ZSTD2");
    CelebornConf celebornConf = new CelebornConf();
    celebornConf.set(PluginConf.MEMORY_PER_RESULT_PARTITION.alterName, "128m");
    celebornConf.set(PluginConf.REMOTE_SHUFFLE_COMPRESSION_CODEC.alterName, "ZSTD");
    Assert.assertEquals(
        "78m", PluginConf.colesce(flinkConf, celebornConf, PluginConf.MEMORY_PER_RESULT_PARTITION));
    Assert.assertEquals(
        "ZSTD2",
        PluginConf.colesce(flinkConf, celebornConf, PluginConf.REMOTE_SHUFFLE_COMPRESSION_CODEC));
  }
}
