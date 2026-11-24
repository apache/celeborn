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

package org.apache.celeborn.service.deploy.master.scale

import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager

/**
 * Lifecycle hooks for a Master autoscaling plugin. Implementations are responsible for honoring
 * HA leadership when performing scaling operations.
 */
trait ScaleManager {

  /**
   * Receives the Master's metadata manager during RPC endpoint startup. Return promptly and
   * schedule long-running work asynchronously.
   */
  def init(metaManager: AbstractMetaManager): Unit

  /** Schedules scaling work asynchronously and returns promptly. */
  def start(): Unit

  /** Stops scaling and releases resources, including after partial startup. */
  def stop(): Unit
}
