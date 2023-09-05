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

import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A helper class to be compatible with Gluten Celeborn.
 */
object GlutenShuffleDependencyHelper {

  def isGlutenDep(depName: String): Boolean = {
    // scalastyle:off
    // see Gluten
    // https://github.com/oap-project/gluten/blob/main/gluten-core/src/main/scala/org/apache/spark/shuffle/ColumnarShuffleDependency.scala
    // scalastyle:on
    "org.apache.spark.shuffle.ColumnarShuffleDependency".equals(depName)
  }

  def withUpdatedRecordsRead(
      input: Iterator[(Any, Any)],
      metrics: ShuffleReadMetricsReporter): Iterator[(Any, Any)] = {
    input.map { record =>
      metrics.incRecordsRead(record._2.asInstanceOf[ColumnarBatch].numRows())
      record
    }
  }
}
