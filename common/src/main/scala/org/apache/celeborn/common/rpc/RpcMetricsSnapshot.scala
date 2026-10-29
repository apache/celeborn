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

package org.apache.celeborn.common.rpc

/**
 * Latency statistics of a single RPC message type, derived from the endpoint's histogram.
 * All time fields are in nanoseconds.
 */
case class RpcMethodMetrics(
    name: String,
    count: Long,
    min: Long,
    mean: Double,
    p50: Double,
    p75: Double,
    p95: Double,
    p99: Double,
    max: Long)

/**
 * Point-in-time RPC metrics of one registered endpoint. The histograms are never reset, so the
 * values are cumulative since the endpoint started.
 */
case class RpcMetricsSnapshot(
    endpointName: String,
    queueSize: Long,
    maxQueueLength: Long,
    methods: Seq[RpcMethodMetrics],
    timestamp: Long)
