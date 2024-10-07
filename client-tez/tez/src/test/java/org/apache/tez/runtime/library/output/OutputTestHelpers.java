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

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.OutputStatisticsReporter;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

final class OutputTestHelpers {

  private OutputTestHelpers() {}

  static OutputContext createOutputContext() throws IOException {
    OutputContext outputContext = mock(OutputContext.class);
    Configuration conf = new TezConfiguration();
    UserPayload payLoad = TezUtils.createUserPayloadFromConf(conf);
    String[] workingDirs = new String[] {"workDir1"};
    OutputStatisticsReporter statsReporter = mock(OutputStatisticsReporter.class);
    TezCounters counters = new TezCounters();

    doReturn("destinationVertex").when(outputContext).getDestinationVertexName();
    doReturn(payLoad).when(outputContext).getUserPayload();
    doReturn(workingDirs).when(outputContext).getWorkDirs();
    doReturn(200 * 1024 * 1024L).when(outputContext).getTotalMemoryAvailableToTask();
    doReturn(counters).when(outputContext).getCounters();
    doReturn(statsReporter).when(outputContext).getStatisticsReporter();
    doReturn(new Configuration(false)).when(outputContext).getContainerConfiguration();
    return outputContext;
  }

  static OutputContext createOutputContext(
      Configuration conf, Configuration userPayloadConf, Path workingDir) throws IOException {
    OutputContext ctx = mock(OutputContext.class);
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) {
                long requestedSize = (Long) invocation.getArguments()[0];
                MemoryUpdateCallbackHandler callback =
                    (MemoryUpdateCallbackHandler) invocation.getArguments()[1];
                callback.memoryAssigned(requestedSize);
                return null;
              }
            })
        .when(ctx)
        .requestInitialMemory(ArgumentMatchers.anyLong(), ArgumentMatchers.any());
    doReturn(conf).when(ctx).getContainerConfiguration();
    doReturn(TezUtils.createUserPayloadFromConf(userPayloadConf)).when(ctx).getUserPayload();
    doReturn("taskVertex").when(ctx).getTaskVertexName();
    doReturn("destinationVertex").when(ctx).getDestinationVertexName();
    doReturn("attempt_1681717153064_3601637_1_13_000096_0").when(ctx).getUniqueIdentifier();
    doReturn(new String[] {workingDir.toString()}).when(ctx).getWorkDirs();
    doReturn(200 * 1024 * 1024L).when(ctx).getTotalMemoryAvailableToTask();
    doReturn(new TezCounters()).when(ctx).getCounters();
    OutputStatisticsReporter statsReporter = mock(OutputStatisticsReporter.class);
    doReturn(statsReporter).when(ctx).getStatisticsReporter();
    doReturn(new ExecutionContextImpl("localhost")).when(ctx).getExecutionContext();
    return ctx;
  }
}
