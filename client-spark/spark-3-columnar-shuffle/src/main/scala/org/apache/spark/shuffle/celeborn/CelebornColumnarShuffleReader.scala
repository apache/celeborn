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

package org.apache.spark.shuffle.celeborn

import org.apache.spark.{ShuffleDependency, TaskContext}
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.execution.columnar.{CelebornBatchBuilder, CelebornColumnarBatchSerializer}

import org.apache.celeborn.common.CelebornConf

class CelebornColumnarShuffleReader[K, C](
    handle: CelebornShuffleHandle[K, _, C],
    startMapIndex: Int = 0,
    endMapIndex: Int = Int.MaxValue,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    conf: CelebornConf,
    metrics: ShuffleReadMetricsReporter)
  extends CelebornShuffleReader[K, C](
    handle,
    startMapIndex,
    endMapIndex,
    startPartition,
    endPartition,
    context,
    conf,
    metrics) {

  override def newSerializerInstance(dep: ShuffleDependency[K, _, C]): SerializerInstance = {
    val schema = CustomShuffleDependencyUtils.getSchema(dep)
    if (CelebornBatchBuilder.supportsColumnarType(
        schema)) {
      val dataSize = SparkUtils.getDataSize(
        dep.serializer.asInstanceOf[UnsafeRowSerializer])
      new CelebornColumnarBatchSerializer(
        schema,
        conf.columnarShuffleBatchSize,
        conf.columnarShuffleDictionaryEnabled,
        conf.columnarShuffleOffHeapEnabled,
        dataSize).newInstance()
    } else {
      super.newSerializerInstance(dep)
    }
  }
}
