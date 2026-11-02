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

package org.apache.celeborn.service.deploy.worker.decommission;

/**
 * Decides whether a worker should decommission itself, based on a signal from outside Celeborn (for
 * example a cloud spot-instance notice, a Kubernetes node drain, or a marker file).
 *
 * <p>The worker loads the implementation named by {@code
 * celeborn.worker.decommission.signal.checker} through a public constructor that takes a single
 * {@code org.apache.celeborn.common.CelebornConf} argument, and then calls {@link
 * #shouldDecommission()} every {@code celeborn.worker.decommission.signal.checkInterval} from a
 * single dedicated thread. When it returns {@code true}, the worker runs the same decommission as
 * {@code POST /api/v1/workers/exit} with type {@code Decommission} and stops polling. The
 * decommission is one-way: a later {@code false} does not recommission the worker.
 *
 * <p>{@link #shouldDecommission()} must not block for long. An exception thrown from it is logged
 * and treated as "no signal". {@link #close()} is called once when the worker stops. The worker
 * waits only briefly for an in-flight {@link #shouldDecommission()} call to return before calling
 * {@link #close()}, so the two may overlap if a check blocks.
 */
public interface DecommissionSignalChecker {

  /** Returns {@code true} when the worker should decommission itself. */
  boolean shouldDecommission() throws Exception;

  /** Optional human-readable reason for the last positive signal; logged by the worker. */
  default String getReason() {
    return null;
  }

  /** Releases any resources held by this checker. */
  default void close() {}
}
