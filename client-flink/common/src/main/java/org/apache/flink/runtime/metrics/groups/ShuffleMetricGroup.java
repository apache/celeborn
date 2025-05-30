/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics.groups;

import static org.apache.flink.runtime.metrics.scope.ShuffleScopeFormat.SCOPE_SHUFFLE_ID;
import static org.apache.flink.runtime.metrics.scope.ShuffleScopeFormat.fromConfig;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Map;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.dump.ShuffleQueryScopeInfo;

/** Special {@link MetricGroup} representing a Flink runtime Shuffle. */
public class ShuffleMetricGroup extends ComponentMetricGroup<TaskMetricGroup> {

  /** The shuffle id uniquely identifying the executed shuffle represented by this metrics group. */
  private final int shuffleId;

  private final ShuffleIOMetricGroup ioMetrics;

  // ------------------------------------------------------------------------

  public ShuffleMetricGroup(MetricGroup taskIOMetricGroup, int shuffleId, String scopeFormat) {
    this(((TaskIOMetricGroup) taskIOMetricGroup).parentMetricGroup, shuffleId, scopeFormat);
  }

  public ShuffleMetricGroup(TaskMetricGroup parent, int shuffleId, String scopeFormat) {
    super(
        parent.registry,
        fromConfig(scopeFormat, parent.registry.getScopeFormats().getTaskFormat())
            .formatScope(checkNotNull(parent), shuffleId),
        parent);
    this.shuffleId = shuffleId;
    this.ioMetrics = new ShuffleIOMetricGroup(this);
  }

  // ------------------------------------------------------------------------
  //  properties
  // ------------------------------------------------------------------------

  public final TaskMetricGroup parent() {
    return parent;
  }

  public int shuffleId() {
    return shuffleId;
  }

  /**
   * Returns the {@link ShuffleIOMetricGroup} for this shuffle.
   *
   * @return {@link ShuffleIOMetricGroup} for this shuffle.
   */
  public ShuffleIOMetricGroup getIOMetricGroup() {
    return ioMetrics;
  }

  @Override
  protected ShuffleQueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
    return new ShuffleQueryScopeInfo(
        this.parent.parent.jobId.toString(),
        this.parent.vertexId.toString(),
        this.parent.subtaskIndex,
        this.parent.attemptNumber(),
        this.shuffleId);
  }

  // ------------------------------------------------------------------------
  //  cleanup
  // ------------------------------------------------------------------------

  @Override
  public void close() {
    super.close();
  }

  // ------------------------------------------------------------------------
  //  Component Metric Group Specifics
  // ------------------------------------------------------------------------

  @Override
  protected void putVariables(Map<String, String> variables) {
    variables.put(SCOPE_SHUFFLE_ID, String.valueOf(shuffleId));
  }

  @Override
  protected Iterable<? extends ComponentMetricGroup<?>> subComponents() {
    return Collections.emptyList();
  }

  @Override
  protected String getGroupName(CharacterFilter filter) {
    return "shuffle";
  }
}
