/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.output;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// Tests which don't require parameterization
public class TestOrderedPartitionedKVOutput {
  private Configuration conf;
  private FileSystem localFs;
  private Path workingDir;

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    localFs = FileSystem.getLocal(conf);
    workingDir =
        new Path(
                System.getProperty("test.build.data", System.getProperty("java.io.tmpdir", "/tmp")),
                TestOrderedPartitionedKVOutput.class.getName())
            .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, Text.class.getName());
    conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, HashPartitioner.class.getName());
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, workingDir.toString());
  }

  @After
  public void cleanup() throws IOException {
    localFs.delete(workingDir, true);
  }

  @Test(timeout = 5000)
  public void testNonStartedOutput() throws IOException {
    OutputContext outputContext = OutputTestHelpers.createOutputContext(conf, conf, workingDir);
    int numPartitions = 10;
    OrderedPartitionedKVOutput output =
        new CelebornOrderedPartitionedKVOutput(outputContext, numPartitions);
    List<Event> events = output.close();
    assertEquals(2, events.size());
    Event event1 = events.get(0);
    assertTrue(event1 instanceof VertexManagerEvent);
    Event event2 = events.get(1);
    assertTrue(event2 instanceof CompositeDataMovementEvent);
    CompositeDataMovementEvent cdme = (CompositeDataMovementEvent) event2;
    ByteBuffer bb = cdme.getUserPayload();
    ShuffleUserPayloads.DataMovementEventPayloadProto shufflePayload =
        ShuffleUserPayloads.DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(bb));
    assertTrue(shufflePayload.hasEmptyPartitions());

    byte[] emptyPartitions =
        TezCommonUtils.decompressByteStringToByteArray(shufflePayload.getEmptyPartitions());
    BitSet emptyPartionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
    assertEquals(numPartitions, emptyPartionsBitSet.cardinality());
    for (int i = 0; i < numPartitions; i++) {
      assertTrue(emptyPartionsBitSet.get(i));
    }
  }

  // @Test(timeout = 5000)
  private void testConfigMerge() throws IOException {
    Configuration localConf = new Configuration(conf);
    localConf.set("config-from-local", "config-from-local-value");
    Configuration payload = new Configuration(false);
    payload.set("config-from-payload", "config-from-payload-value");
    OutputContext outputContext =
        OutputTestHelpers.createOutputContext(localConf, payload, workingDir);
    int numPartitions = 10;
    OrderedPartitionedKVOutput output =
        new CelebornOrderedPartitionedKVOutput(outputContext, numPartitions);
    output.initialize();
    Configuration configAfterMerge = output.conf;
    assertEquals("config-from-local-value", configAfterMerge.get("config-from-local"));
    assertEquals("config-from-payload-value", configAfterMerge.get("config-from-payload"));
  }

  // @Test(timeout = 10000)
  private void testClose() throws Exception {
    OutputContext outputContext = OutputTestHelpers.createOutputContext(conf, conf, workingDir);
    int numPartitions = 10;
    OrderedPartitionedKVOutput output =
        new CelebornOrderedPartitionedKVOutput(outputContext, numPartitions);
    output.start();
    output.close();
  }
}
