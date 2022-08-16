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

package org.apache.spark.shuffle.rss

import org.apache.spark._
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.Utils

object RssSparkUtils {

  // Create an instance of the class with the given name, possibly initializing it with our conf
  // Copied from SparkEnv
  def instantiateClass[T](className: String, conf: SparkConf, isDriver: Boolean): T = {
    val cls = Utils.classForName(className)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }

  // Invoke and return getReader method of SortShuffleManager
  def invokeGetReaderMethod[K, C](
      className: String,
      methodName: String,
      sortShuffleManager: SortShuffleManager,
      handle: ShuffleHandle,
      startMapIndex: Int = 0,
      endMapIndex: Int = Int.MaxValue,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val cls = Utils.classForName(className)
    try {
      val method = cls.getMethod(methodName, classOf[ShuffleHandle], Integer.TYPE, Integer.TYPE,
        Integer.TYPE, Integer.TYPE, classOf[TaskContext], classOf[ShuffleReadMetricsReporter])
      method.invoke(
        sortShuffleManager,
        handle,
        Integer.valueOf(startMapIndex),
        Integer.valueOf(endMapIndex),
        Integer.valueOf(startPartition),
        Integer.valueOf(endPartition),
        context,
        metrics).asInstanceOf[ShuffleReader[K, C]]
    } catch {
      case _: NoSuchMethodException =>
        try {
          val method = cls.getMethod(methodName, classOf[ShuffleHandle], Integer.TYPE, Integer.TYPE,
            classOf[TaskContext], classOf[ShuffleReadMetricsReporter])
          method.invoke(
            sortShuffleManager,
            handle,
            Integer.valueOf(startPartition),
            Integer.valueOf(endPartition),
            context,
            metrics).asInstanceOf[ShuffleReader[K, C]]
        } catch {
          case e: NoSuchMethodException =>
            throw new Exception("Get getReader method failed.", e)
        }
    }
  }
}
