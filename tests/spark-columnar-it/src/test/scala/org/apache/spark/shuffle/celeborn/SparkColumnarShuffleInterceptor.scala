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

import net.bytebuddy.agent.ByteBuddyAgent
import net.bytebuddy.dynamic.loading.{ClassLoadingStrategy, ClassReloadingStrategy}
import net.bytebuddy.pool.TypePool
import net.bytebuddy.ByteBuddy
import net.bytebuddy.description.modifier.Visibility
import net.bytebuddy.dynamic.ClassFileLocator
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy
import net.bytebuddy.implementation.{FieldAccessor, MethodCall, MethodDelegation}
import net.bytebuddy.matcher.ElementMatchers
import org.apache.spark.util.Utils

object SparkColumnarShuffleInterceptor {

  def install(): Unit = {

    ByteBuddyAgent
      .install()


    val typePool = TypePool.Default.ofSystemLoader()
    val classReloadingStrategy = ClassReloadingStrategy.fromInstalledAgent()

    val clz = new ByteBuddy()
      .redefine(typePool.describe("org.apache.spark.ShuffleDependency").resolve(), // do not use 'Bar.class'
        ClassFileLocator.ForClassLoader.ofSystemLoader())
      .defineField("schema", classOf[org.apache.spark.sql.types.StructType], Visibility.PUBLIC)
      .make()
      .load(Utils.getSparkClassLoader, ClassLoadingStrategy.Default.INJECTION)
      .getLoaded

    val types2: Seq[Class[_]] = Seq(
      classOf[org.apache.spark.rdd.RDD[_]],
      classOf[org.apache.spark.Partitioner],
      classOf[org.apache.spark.serializer.Serializer],
      classOf[scala.Option[_]],
      classOf[scala.Option[_]],
      classOf[Boolean],
      classOf[org.apache.spark.sql.types.StructType],
      classOf[org.apache.spark.shuffle.ShuffleWriteProcessor],
      classOf[scala.reflect.ClassTag[_]],
      classOf[scala.reflect.ClassTag[_]],
      classOf[scala.reflect.ClassTag[_]])
    val parameterTypes: java.util.List[Class[_]] = java.util.Arrays.asList[Class[_]](types2: _*)
    new ByteBuddy()
      .subclass(clz, ConstructorStrategy.Default.NO_CONSTRUCTORS)
      .name("org.apache.spark.CelebornColumnarShuffleDependency")
      .defineConstructor(Visibility.PUBLIC)
      .withParameters(parameterTypes)
      .intercept(
        MethodCall.invoke(
          Class.forName("org.apache.spark.ShuffleDependency").getDeclaredConstructors.head)
          .withArgument(0, 1, 2, 3, 4, 5, 7, 8, 9, 10)
          .andThen(FieldAccessor.ofField("schema").setsArgumentAt(6))
      )
      .make()
      .load(Utils.getSparkClassLoader, classReloadingStrategy)


    new ByteBuddy()
      .rebase(typePool.describe("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec$").resolve(), // do not use 'Bar.class'
        ClassFileLocator.ForClassLoader.ofSystemLoader())
      .method(ElementMatchers.named("prepareShuffleDependency"))
      .intercept(MethodDelegation.to(classOf[JavaShuffleExchangeExecInterceptor]))
      .make()
      .load(ClassLoader.getSystemClassLoader(), classReloadingStrategy)
  }

}
