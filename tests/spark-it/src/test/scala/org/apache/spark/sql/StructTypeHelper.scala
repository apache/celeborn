package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.StructType

/**
 * @time 2023/11/29 12:57 AM
 * @author fchen <cloud.chenfu@gmail.com>
 */
object StructTypeHelper {
  def fromAttributes(attributes: Seq[Attribute]): StructType =
    StructType.fromAttributes(attributes)

}
