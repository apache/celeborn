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

package org.apache.celeborn.common.network.protocol;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import com.google.protobuf.ExtensionRegistry;

import org.apache.celeborn.common.protocol.TransportMessages;
import org.apache.celeborn.common.util.ThreadUtils;

public class TransportMessagesHelper {

  private final ScheduledExecutorService transportMessagesRunner;
  private final Future<?> runTransportMessagesStaticBlockerTask;

  public TransportMessagesHelper() {
    transportMessagesRunner =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("transport-messages-runner");
    runTransportMessagesStaticBlockerTask =
        transportMessagesRunner.submit(
            () ->
                // Pre-run TransportMessages static code blocks to improve performance of protobuf
                // serialization.
                TransportMessages.registerAllExtensions(ExtensionRegistry.newInstance()));
  }

  public void close() {
    runTransportMessagesStaticBlockerTask.cancel(true);
    ThreadUtils.shutdown(transportMessagesRunner);
  }
}
