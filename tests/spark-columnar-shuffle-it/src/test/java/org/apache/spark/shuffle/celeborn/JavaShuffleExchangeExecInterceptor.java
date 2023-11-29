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

package org.apache.spark.shuffle.celeborn;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.spark.sql.StructTypeHelper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;

public class JavaShuffleExchangeExecInterceptor {
  @RuntimeType
  public static Object intercept(@Argument(1) Object outputAttributes, @SuperCall Callable<Object> shuffleExchangeExec) {
    try {
      Object shuffleDependency = shuffleExchangeExec.call();

      return Class.forName("org.apache.spark.CelebornColumnarShuffleDependency")
          .getConstructors()[0]
          .newInstance(
              getField(shuffleDependency, "_rdd"),
              getField(shuffleDependency, "partitioner"),
              getField(shuffleDependency, "serializer"),
              getField(shuffleDependency, "keyOrdering"),
              getField(shuffleDependency, "aggregator"),
              getField(shuffleDependency, "mapSideCombine"),
              StructTypeHelper.fromAttributes((scala.collection.Seq<Attribute>) outputAttributes),
              // Spark 3.5
              // DataTypeUtils.fromAttributes(args(1).asInstanceOf[Seq[Attribute]]),
              getField(shuffleDependency, "shuffleWriterProcessor"),
              scala.reflect.ClassTag.Int(),
              scala.reflect.ClassTag.apply(InternalRow.class),
              scala.reflect.ClassTag.apply(InternalRow.class)
          );
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  private static Object getField(Object obj, String fieldName)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(obj);
  }
}
