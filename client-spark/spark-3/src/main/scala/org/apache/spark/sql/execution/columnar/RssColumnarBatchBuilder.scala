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

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

class RssColumnarBatchBuilder(
    schema: StructType,
    batchSize: Int = 0,
    maxDictFactor: Double,
    useCompression: Boolean = false) {
  var rowCnt = 0

  val typeConversion: PartialFunction[DataType, NativeRssColumnType[_ <: AtomicType]] = {
    case IntegerType => RSS_INT
    case LongType => RSS_LONG
    case StringType => RSS_STRING
    case BooleanType => RSS_BOOLEAN
    case ShortType => RSS_SHORT
    case ByteType => RSS_BYTE
    case FloatType => RSS_FLOAT
    case DoubleType => RSS_DOUBLE
    case dt: DecimalType if dt.precision <= Decimal.MAX_INT_DIGITS => RSS_COMPACT_MINI_DECIMAL(dt)
    case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS => RSS_COMPACT_DECIMAL(dt)
    case _ => null
  }

  val encodersArr: Array[Encoder[_ <: AtomicType]] = schema.map { attribute =>
    val nativeColumnType = typeConversion(attribute.dataType)
    if (nativeColumnType == null) {
      null
    } else {
      if (useCompression && RssDictionaryEncoding.supports(nativeColumnType)) {
        RssDictionaryEncoding.MAX_DICT_SIZE =
          Math.min(Short.MaxValue, batchSize * maxDictFactor).toShort
        RssDictionaryEncoding.encoder(nativeColumnType)
      } else {
        RssPassThrough.encoder(nativeColumnType)
      }
    }
  }.toArray

  var columnBuilders: Array[RssColumnBuilder] = _

  def newBuilders(): Unit = {
    totalSize = 0
    rowCnt = 0
    var i = -1
    columnBuilders = schema.map { attribute =>
      i += 1
      encodersArr(i) match {
        case encoder: RssDictionaryEncoding.RssEncoder[_] if !encoder.overflow =>
          encoder.cleanBatch
        case _ =>
      }
      RssColumnBuilder(
        attribute.dataType,
        batchSize,
        attribute.name,
        useCompression,
        encodersArr(i))
    }.toArray
  }

  def buildColumnBytes(): Array[Byte] = {
    var offset = 0
    val giantBuffer = new Array[Byte](totalSize)
    val rowCntBytes = int2ByteArray(rowCnt)
    System.arraycopy(rowCntBytes, 0, giantBuffer, offset, rowCntBytes.length)
    offset += 4
    columnBuilders.foreach { builder =>
      val buffers = builder.build()
      val bytes = JavaUtils.bufferToArray(buffers)
      val columnBuilderBytes = int2ByteArray(bytes.length)
      System.arraycopy(columnBuilderBytes, 0, giantBuffer, offset, columnBuilderBytes.length)
      offset += 4
      System.arraycopy(bytes, 0, giantBuffer, offset, bytes.length)
      offset += bytes.length
    }
    giantBuffer
  }

  def int2ByteArray(i: Int): Array[Byte] = {
    val result = new Array[Byte](4)
    result(0) = ((i >> 24) & 0xFF).toByte
    result(1) = ((i >> 16) & 0xFF).toByte
    result(2) = ((i >> 8) & 0xFF).toByte
    result(3) = (i & 0xFF).toByte
    result
  }

  var totalSize = 0

  def writeRow(row: InternalRow): Unit = {
    var i = 0
    while (i < row.numFields) {
      columnBuilders(i).appendFrom(row, i)
      i += 1
    }
    rowCnt += 1
  }

  def getTotalSize(): Int = {
    var i = 0
    var tempTotalSize = 0
    while (i < schema.length) {
      columnBuilders(i) match {
        case builder: RssCompressibleColumnBuilder[_] =>
          tempTotalSize += builder.getTotalSize.toInt
        case builder: RssNullableColumnBuilder => tempTotalSize += builder.getTotalSize.toInt
        case _ =>
      }
      i += 1
    }
    totalSize = tempTotalSize + 4 + 4 * schema.length
    totalSize
  }
}

object RssColumnarBatchBuilder {
  def supportsColumnarType(schema: StructType): Boolean = {
    schema.fields.forall(f =>
      f.dataType match {
        case BooleanType | ByteType | ShortType | IntegerType | LongType |
            FloatType | DoubleType | StringType => true
        case dt: DecimalType => true
        case _ => false
      })
  }
}
