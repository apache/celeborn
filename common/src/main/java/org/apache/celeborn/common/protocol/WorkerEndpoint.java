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

package org.apache.celeborn.common.protocol;

import java.io.Serializable;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

/**
 * Immutable and weakly interned worker endpoint shared by partition locations. Java serialization
 * is retained for same-version internal use; its serialized form is not a cross-version
 * compatibility contract.
 */
public final class WorkerEndpoint implements Serializable {
  private static final Interner<WorkerEndpoint> INTERNED_ENDPOINTS = Interners.newWeakInterner();

  private final String host;
  private final int rpcPort;
  private final int pushPort;
  private final int fetchPort;
  private final int replicatePort;
  private transient volatile String hostPushPort;
  private transient volatile String hostFetchPort;

  private WorkerEndpoint(String host, int rpcPort, int pushPort, int fetchPort, int replicatePort) {
    this.host = host;
    this.rpcPort = rpcPort;
    this.pushPort = pushPort;
    this.fetchPort = fetchPort;
    this.replicatePort = replicatePort;
  }

  public static WorkerEndpoint apply(
      String host, int rpcPort, int pushPort, int fetchPort, int replicatePort) {
    WorkerEndpoint endpoint = new WorkerEndpoint(host, rpcPort, pushPort, fetchPort, replicatePort);
    return INTERNED_ENDPOINTS.intern(endpoint);
  }

  public String host() {
    return host;
  }

  public int rpcPort() {
    return rpcPort;
  }

  public int pushPort() {
    return pushPort;
  }

  public int fetchPort() {
    return fetchPort;
  }

  public int replicatePort() {
    return replicatePort;
  }

  public String hostAndPushPort() {
    String current = hostPushPort;
    if (current == null) {
      current = host + ":" + pushPort;
      hostPushPort = current;
    }
    return current;
  }

  public String hostAndFetchPort() {
    String current = hostFetchPort;
    if (current == null) {
      current = host + ":" + fetchPort;
      hostFetchPort = current;
    }
    return current;
  }

  private Object readResolve() {
    return apply(host, rpcPort, pushPort, fetchPort, replicatePort);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof WorkerEndpoint)) {
      return false;
    }
    WorkerEndpoint that = (WorkerEndpoint) other;
    return rpcPort == that.rpcPort
        && pushPort == that.pushPort
        && fetchPort == that.fetchPort
        && replicatePort == that.replicatePort
        && host.equals(that.host);
  }

  @Override
  public int hashCode() {
    int result = host.hashCode();
    result = 31 * result + rpcPort;
    result = 31 * result + pushPort;
    result = 31 * result + fetchPort;
    result = 31 * result + replicatePort;
    return result;
  }
}
