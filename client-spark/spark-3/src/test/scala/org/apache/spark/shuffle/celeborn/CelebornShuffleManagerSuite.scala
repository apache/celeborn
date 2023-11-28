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

import java.util.concurrent.Callable

import net.bytebuddy.agent.builder.AgentBuilder
import net.bytebuddy.asm.Advice
import net.bytebuddy.dynamic.ClassFileLocator
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy
import net.bytebuddy.implementation.{FieldAccessor, MethodDelegation}
import net.bytebuddy.implementation.bind.annotation.{AllArguments, Argument, Origin, RuntimeType, SuperCall}
import net.bytebuddy.matcher.ElementMatchers
import net.bytebuddy.pool.TypePool
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.internal.SQLConf
import org.junit
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
//import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

@RunWith(classOf[JUnit4])
class SparkShuffleManagerSuite extends Logging {
  @junit.Test
  def testFallBack(): Unit = {
    val conf = new SparkConf().setIfMissing("spark.master", "local")
      .setIfMissing(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", "localhost:9097")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "false")
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.shuffle.useOldFetchProtocol", "true")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .setAppName("test")
    val sc = new SparkContext(conf)
    // scalastyle:off println
    sc.parallelize(1 to 1000, 2).map { i => (i, Range(1, 100).mkString(",")) }
      .groupByKey(16).count()
    // scalastyle:on println
    sc.stop()
  }

  @junit.Test
  def testClusterNotAvailable(): Unit = {
    val conf = new SparkConf().setIfMissing("spark.master", "local")
      .setIfMissing(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", "localhost:9097")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "false")
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.shuffle.useOldFetchProtocol", "true")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .setAppName("test")
    val sc = new SparkContext(conf)
    // scalastyle:off println
    sc.parallelize(1 to 1000, 2).map { i => (i, Range(1, 100).mkString(",")) }
      .groupByKey(16).count()
    // scalastyle:on println
    sc.stop()
  }

//  @junit.Test
//  def tt(): Unit = {
//    import net.bytebuddy.ByteBuddy
//    import net.bytebuddy.agent.ByteBuddyAgent
//    import net.bytebuddy.dynamic.loading.ClassReloadingStrategy
//    import net.bytebuddy.implementation.MethodCall
//    import net.bytebuddy.matcher.ElementMatchers.named
//    import net.bytebuddy.description.modifier.Visibility
//
////    new ByteBuddy()
////      .redefine(classOf[org.apache.spark.sql.execution.exchange.ShuffleExchangeExec])
////    .defineField("_id", int.class, Visibility.PUBLIC)
////    .make()
////      .load(MyDocument.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());
////
////    classOf[org.apache.spark.sql.execution.exchange.ShuffleExchangeExec].getDeclaredFields
////      .foreach(f => println(f.getName))
////    println("1====")
////    classOf[org.apache.spark.sql.execution.exchange.ShuffleExchangeExec].getDeclaredConstructors
////      .foreach { c =>
////        println("=====")
////        c.getParameterTypes.foreach(t => println(t.getName))
////      }
//
//    ByteBuddyAgent
//      .install()
////     new AgentBuilder.Default()
////            .disableClassFormatChanges()
////            .with(AgentBuilder.RedefinitionStrategy.REDEFINITION)
////            .type(ElementMatchers.named("pkg.MapText"))
////            .transform(new AgentBuilder.Transformer() {
////                @Override
////                public DynamicType.Builder<?> transform(DynamicType.Builder<?> builder, TypeDescription typeDescription, ClassLoader classLoader, JavaModule javaModule) {
////                    return builder.defineMethod("save", void.class, Visibility.PUBLIC)
////                            .intercept(MethodDelegation.to(ActiveRecordInterceptor.class));
////                }
////            }).with(new ListenerImpl()).installOnByteBuddyAgent();
////    CallTextSave callTextSave =  new CallTextSave();
////    callTextSave.save();
//
//
//
//    val typePool = TypePool.Default.ofSystemLoader()
//    val classLoader = Utils.getContextOrSparkClassLoader
//    val classReloadingStrategy = ClassReloadingStrategy.fromInstalledAgent()
//
//    // 方案1
//    val clz = new ByteBuddy()
//      .redefine(typePool.describe("org.apache.spark.ShuffleDependency").resolve(), // do not use 'Bar.class'
//                ClassFileLocator.ForClassLoader.ofSystemLoader())
////      .redefine(classOf[org.apache.spark.ShuffleDependency[_, _, _]])
////      .field(named"xyz")
//      .defineField("schema", classOf[org.apache.spark.sql.types.StructType], Visibility.PUBLIC)
//      .make()
////      .load(classLoader, classReloadingStrategy)
//      .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.INJECTION)
//      .getLoaded
//
//
//    // 方案2
//    val types1: Seq[Class[_]] = Seq(
//      classOf[org.apache.spark.rdd.RDD[_]],
//      classOf[org.apache.spark.Partitioner],
//      classOf[org.apache.spark.serializer.Serializer],
//      classOf[scala.Option[_]],
//      classOf[scala.Option[_]],
//      classOf[Boolean],
//      classOf[org.apache.spark.shuffle.ShuffleWriteProcessor],
//      classOf[scala.reflect.ClassTag[_]],
//      classOf[scala.reflect.ClassTag[_]],
//      classOf[scala.reflect.ClassTag[_]])
//    val types2: Seq[Class[_]] = Seq(
//      classOf[org.apache.spark.rdd.RDD[_]],
//      classOf[org.apache.spark.Partitioner],
//      classOf[org.apache.spark.serializer.Serializer],
//      classOf[scala.Option[_]],
//      classOf[scala.Option[_]],
//      classOf[Boolean],
//      classOf[org.apache.spark.sql.types.StructType],
//      classOf[org.apache.spark.shuffle.ShuffleWriteProcessor],
//      classOf[scala.reflect.ClassTag[_]],
//      classOf[scala.reflect.ClassTag[_]],
//      classOf[scala.reflect.ClassTag[_]])
//    val parameterTypes: java.util.List[Class[_]] = java.util.Arrays.asList[Class[_]](types2: _*)
//    val columnarClz = new ByteBuddy()
//
//      .subclass(clz, ConstructorStrategy.Default.NO_CONSTRUCTORS)
////      .redefine(typePool.describe("org.apache.spark.ShuffleDependency").resolve(), // do not use 'Bar.class'
////        ClassFileLocator.ForClassLoader.ofSystemLoader())
////      .redefine(Class.forName("org.apache.spark.ShuffleDependency"))
////      .redefine(classOf[org.apache.spark.ShuffleDependency[_, _, _]])
////      .redefine(clz)
////      .defineField("schema", classOf[org.apache.spark.sql.types.StructType], Visibility.PUBLIC)
//      .name("org.apache.spark.CelebornColumnarShuffleDependency")
//      .defineConstructor(Visibility.PUBLIC)
//      .withParameters(parameterTypes)
//      .intercept(
//        MethodCall.invoke(
////          Class.forName("org.apache.spark.ShuffleDependency").getConstructor(types1: _*))
//          Class.forName("org.apache.spark.ShuffleDependency").getDeclaredConstructors.head)
//          .withArgument(0, 1, 2, 3, 4, 5, 7, 8, 9, 10)
//          .andThen(FieldAccessor.ofField("schema").setsArgumentAt(6))
//      )
//      .make()
////      .load(ClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.INJECTION)
//      .load(ClassLoader.getSystemClassLoader(), classReloadingStrategy)
//      .getLoaded
//
//
//    classOf[org.apache.spark.ShuffleDependency[_, _, _]].getDeclaredFields
//      .foreach(f => println(f.getName))
//    classOf[org.apache.spark.ShuffleDependency[_, _, _]].getDeclaredConstructors
//          .foreach { c =>
//            println("=====")
//            c.getParameterTypes.foreach(t => println(t.getName))
//          }
//
//    println("columnar")
//    Class.forName("org.apache.spark.CelebornColumnarShuffleDependency")
//      .getDeclaredConstructors
//      .foreach { c =>
//        println("=====")
//        c.getParameterTypes.foreach(t => println(t.getName))
//      }
//
//
//      val method = classOf[JavaShuffleExchangeExecInterceptor]
//        .getDeclaredMethods
//        .filter(m => m.getName == "intercept")
//        .head
////    new ByteBuddy()
////      .redefine(ShuffleExchangeExec.getClass)
//////      .redefine(Class.forName("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec$"))
////      .method(ElementMatchers.named("prepareShuffleDependency"))
////      .intercept(MethodDelegation.to(new JavaShuffleExchangeExecInterceptor))
//////      .intercept(MethodCall.invoke(method))
////      .make()
////      .load(ClassLoader.getSystemClassLoader(), classReloadingStrategy)
//
//    new ByteBuddy()
//      .redefine(ShuffleExchangeExec.getClass)
//      .visit(Advice.to(classOf[JavaShuffleExchangeExecInterceptorV2]).on(named("prepareShuffleDependency")))
//      .make()
//      .load(ClassLoader.getSystemClassLoader(), classReloadingStrategy)
//
//
//  }
//
//  @junit.Test
//  def t2(): Unit = {
////    classOf[org.apache.spark.ShuffleDependency[_, _, _]].getDeclaredFields
////      .foreach(f => println(f.getName))
////    classOf[org.apache.spark.ShuffleDependency[_, _, _]].getDeclaredConstructors
////      .foreach { c =>
////        println("=====")
////        println(c.getParameterCount)
////        c.getParameterTypes.foreach(t => println(t.getName))
////      }
////
////      ShuffleExchangeExec.getClass.getDeclaredMethods.foreach(m => println(m.getName))
//    Class.forName("org.apache.spark.sql.execution.exchange.ShuffleExchangeExec")
//      .getDeclaredMethods
//      .foreach(m => println(m.getName))
//
//  }
}

//class ShuffleExchangeExecInterceptor {
//
//  @RuntimeType
//  def intercept(
////      @Argument(value = 1) outputAttributes: Any,
//      @AllArguments args: Array[Any],
//      @SuperCall shuffleExchangeExec: Callable[_]): Any = {
//    val shuffleDependency = shuffleExchangeExec.call()
//    val getField = (name: String) => {
//      val f = shuffleExchangeExec.getClass.getDeclaredField(name)
//      f.setAccessible(true)
//      f.get(shuffleDependency)
//    }
//    Class.forName("org.apache.spark.CelebornColumnarShuffleDependency")
//      .getConstructors
//      .head
//      .newInstance(
//        Seq(
//          getField("_rdd"),
//          getField("partitioner"),
//          getField("serializer"),
//          getField("keyOrdering"),
//          getField("aggregator"),
//          getField("mapSideCombine"),
//          DataTypeUtils.fromAttributes(args(1).asInstanceOf[Seq[Attribute]]),
//          getField("shuffleWriterProcessor"),
//          scala.reflect.ClassTag.Int,
//          scala.reflect.classTag[InternalRow],
//          scala.reflect.classTag[InternalRow]).map(_.asInstanceOf[Object]): _*
//      )
//  }
//}
