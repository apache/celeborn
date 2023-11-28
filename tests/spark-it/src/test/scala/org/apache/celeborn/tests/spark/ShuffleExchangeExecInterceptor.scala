package org.apache.celeborn.tests.spark

import org.scalatest.funsuite.AnyFunSuite
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
//import org.apache.spark.shuffle.celeborn.{JavaShuffleExchangeExecInterceptor, JavaShuffleExchangeExecInterceptorV2}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.StructTypeHelper
//import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils



class ShuffleExchangeExecInterceptor {

  @RuntimeType
  def intercept(
                 //      @Argument(value = 1) outputAttributes: Any,
                 @AllArguments args: Array[Any],
                 @SuperCall shuffleExchangeExec: Callable[_]): Any = {
    val shuffleDependency = shuffleExchangeExec.call()
    val getField = (name: String) => {
      val f = shuffleExchangeExec.getClass.getDeclaredField(name)
      f.setAccessible(true)
      f.get(shuffleDependency)
    }
    Class.forName("org.apache.spark.CelebornColumnarShuffleDependency")
      .getConstructors
      .head
      .newInstance(
        Seq(
          getField("_rdd"),
          getField("partitioner"),
          getField("serializer"),
          getField("keyOrdering"),
          getField("aggregator"),
          getField("mapSideCombine"),
          StructTypeHelper.fromAttributes(args(1).asInstanceOf[Seq[Attribute]]),
          getField("shuffleWriterProcessor"),
          scala.reflect.ClassTag.Int,
          scala.reflect.classTag[InternalRow],
          scala.reflect.classTag[InternalRow]).map(_.asInstanceOf[Object]): _*
      )
  }
}
