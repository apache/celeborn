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

package org.apache.spark.sql.execution.columnar

import java.io.ByteArrayOutputStream

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.types._

class CelebornColumnarBatchBuilder(
    schema: StructType,
    batchSize: Int = 0,
    maxDictFactor: Double,
    encodingEnabled: Boolean = false) extends CelebornBatchBuilder {
  var rowCnt = 0

  private val typeConversion
      : PartialFunction[DataType, NativeCelebornColumnType[_ <: PhysicalDataType]] = {
    case IntegerType => CELEBORN_INT
    case LongType => CELEBORN_LONG
    case StringType => CELEBORN_STRING
    case BooleanType => CELEBORN_BOOLEAN
    case ShortType => CELEBORN_SHORT
    case ByteType => CELEBORN_BYTE
    case FloatType => CELEBORN_FLOAT
    case DoubleType => CELEBORN_DOUBLE
    case dt: DecimalType if dt.precision <= Decimal.MAX_INT_DIGITS =>
      CELEBORN_COMPACT_MINI_DECIMAL(dt)
    case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS => CELEBORN_COMPACT_DECIMAL(dt)
    case _ => null
  }

  private val encodersArr: Array[Encoder[_ <: PhysicalDataType]] = schema.map { attribute =>
    val nativeColumnType = typeConversion(attribute.dataType)
    if (nativeColumnType == null) {
      null
    } else {
      if (encodingEnabled && CelebornDictionaryEncoding.supports(nativeColumnType)) {
        CelebornDictionaryEncoding.MAX_DICT_SIZE =
          Math.min(Short.MaxValue, batchSize * maxDictFactor).toShort
        CelebornDictionaryEncoding.encoder(nativeColumnType)
      } else {
        CelebornPassThrough.encoder(nativeColumnType)
      }
    }
  }.toArray

  var columnBuilders: Array[CelebornColumnBuilder] = _

  def newBuilders(): Unit = {
    rowCnt = 0
    var i = -1
    columnBuilders = schema.map { attribute =>
      i += 1
      encodersArr(i) match {
        case encoder: CelebornDictionaryEncoding.CelebornEncoder[_] if !encoder.overflow =>
          encoder.cleanBatch
        case _ =>
      }
      CelebornColumnBuilder(
        attribute.dataType,
        batchSize,
        attribute.name,
        encodingEnabled,
        encodersArr(i))
    }.toArray
  }

  def buildColumnBytes(): Array[Byte] = {
    val giantBuffer = new ByteArrayOutputStream
    val rowCntBytes = int2ByteArray(rowCnt)
    giantBuffer.write(rowCntBytes)
    val builderLen = columnBuilders.length
    var i = 0
    while (i < builderLen) {
      val builder = columnBuilders(i)
      val buffers = builder.build()
      val bytes = JavaUtils.bufferToArray(buffers)
      val columnBuilderBytes = int2ByteArray(bytes.length)
      giantBuffer.write(columnBuilderBytes)
      giantBuffer.write(bytes)
      i += 1
    }
    giantBuffer.toByteArray
  }

  def writeRow(row: InternalRow): Unit = {
    var i = 0
    while (i < row.numFields) {
      columnBuilders(i).appendFrom(row, i)
      i += 1
    }
    rowCnt += 1
  }

  def getRowCnt: Int = rowCnt
}
