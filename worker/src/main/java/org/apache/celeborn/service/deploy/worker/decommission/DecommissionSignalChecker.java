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
 * <p>Lifecycle: the worker loads the implementation named by {@code
 * celeborn.worker.decommission.signal.checker} through a public constructor that takes a single
 * {@code org.apache.celeborn.common.CelebornConf} argument. It then calls {@link
 * #shouldDecommission()} every {@code celeborn.worker.decommission.signal.checkInterval} from a
 * single dedicated thread, so calls never overlap. The first time it returns {@code true}, the
 * worker runs the same decommission as {@code POST /api/v1/workers/exit} with type {@code
 * Decommission}, stops polling, and only then calls {@link #getReason()} for its log. The
 * decommission is one-way: a later {@code false} does not recommission the worker. When the worker
 * stops, it interrupts the polling thread, waits briefly for an in-flight check, and then calls
 * {@link #close()} once.
 *
 * <p>Contract for implementations:
 *
 * <ul>
 *   <li>{@link #shouldDecommission()}, {@link #getReason()} and {@link #close()} must all return
 *       quickly and put their own timeouts on any I/O. The worker does not time them out. A check
 *       that blocks forever means the signal is never acted on.
 *   <li>An exception from {@link #shouldDecommission()} is logged and treated as "no signal".
 *   <li>{@link #close()} can run while a {@link #shouldDecommission()} call is still in flight, if
 *       that call ignored the interrupt and outlived the worker's wait. So {@link #close()} must be
 *       thread-safe against an in-flight check and must not wait for it. The worker also stops
 *       waiting for {@link #close()} after a short time and continues shutting down.
 * </ul>
 */
public interface DecommissionSignalChecker {

  /** Returns {@code true} when the worker should decommission itself. */
  boolean shouldDecommission() throws Exception;

  /**
   * Optional human-readable reason for the last positive signal. Called once, after the
   * decommission is triggered, and only logged.
   */
  default String getReason() {
    return null;
  }

  /**
   * Releases any resources held by this checker. Called once when the worker stops, possibly while
   * a {@link #shouldDecommission()} call is still running.
   */
  default void close() {}
}
