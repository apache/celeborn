package org.apache.spark.shuffle.celeborn;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.spark.sql.StructTypeHelper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
//import org.apache.spark.sql.catalyst.types.DataTypeUtils;

/**
 * @author fchen <cloud.chenfu@gmail.com>
 * @time 2023/11/28 5:43 PM
 */
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
              //                              DataTypeUtils.fromAttributes(args(1).asInstanceOf[Seq[Attribute]]),
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

  //  @RuntimeType
  //  def intercept(
  //      //      @Argument(value = 1) outputAttributes: Any,
  //      @AllArguments args: Array[Any],
  //      @SuperCall shuffleExchangeExec: Callable[_]): Any = {
  //    val shuffleDependency = shuffleExchangeExec.call()
  //    val getField = (name: String) => {
  //      val f = shuffleExchangeExec.getClass.getDeclaredField(name)
  //      f.setAccessible(true)
  //      f.get(shuffleDependency)
  //    }
  //    Class.forName("org.apache.spark.CelebornColumnarShuffleDependency")
  //        .getConstructors
  //        .head
  //        .newInstance(
  //            Seq(
  //                getField("_rdd"),
  //                getField("partitioner"),
  //                getField("serializer"),
  //                getField("keyOrdering"),
  //                getField("aggregator"),
  //                getField("mapSideCombine"),
  //                DataTypeUtils.fromAttributes(args(1).asInstanceOf[Seq[Attribute]]),
  //                getField("shuffleWriterProcessor"),
  //                scala.reflect.ClassTag.Int,
  //                scala.reflect.classTag[InternalRow],
  //                scala.reflect.classTag[InternalRow]).map(_.asInstanceOf[Object]): _*
  //      )
  //  }
  //}
}
