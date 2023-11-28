package org.apache.spark.shuffle.celeborn;

import java.lang.reflect.Field;

import net.bytebuddy.asm.Advice;
import org.apache.spark.sql.StructTypeHelper;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
//import org.apache.spark.sql.catalyst.types.DataTypeUtils;

/**
 * @author fchen <cloud.chenfu@gmail.com>
 * @time 2023/11/28 10:38 PM
 */
public class JavaShuffleExchangeExecInterceptorV2 {
  @Advice.OnMethodExit
  public static Object intercept(
      @Advice.Return Object shuffleDependency,
      @Advice.Argument(1) Object outputAttributes) {
    try {
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

  public static Object getField(Object obj, String fieldName)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(obj);
  }
}

