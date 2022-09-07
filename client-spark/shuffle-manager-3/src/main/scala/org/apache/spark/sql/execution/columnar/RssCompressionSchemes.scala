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

import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._

case object RssPassThrough extends RssCompressionScheme {
  override val typeId = 0

  override def supports(columnType: RssColumnType[_]): Boolean = true

  override def encoder[T <: AtomicType](columnType: NativeRssColumnType[T]): Encoder[T] = {
    new this.RssEncoder[T](columnType)
  }

  override def decoder[T <: AtomicType](
      buffer: ByteBuffer,
      columnType: NativeRssColumnType[T]): Decoder[T] = {
    new this.RssDecoder(buffer, columnType)
  }

  class RssEncoder[T <: AtomicType](columnType: NativeRssColumnType[T]) extends Encoder[T] {
    override def uncompressedSize: Int = 0

    override def compressedSize: Int = 0

    override def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer = {
      // Writes compression type ID and copies raw contents
      to.putInt(RssPassThrough.typeId).put(from).rewind()
      to
    }
  }

  class RssDecoder[T <: AtomicType](buffer: ByteBuffer, columnType: NativeRssColumnType[T])
    extends Decoder[T] {

    override def next(row: InternalRow, ordinal: Int): Unit = {
      columnType.extract(buffer, row, ordinal)
    }

    override def hasNext: Boolean = buffer.hasRemaining

    private def putBooleans(
        columnVector: WritableColumnVector,
        pos: Int,
        bufferPos: Int,
        len: Int): Unit = {
      for (i <- 0 until len) {
        columnVector.putBoolean(pos + i, buffer.get(bufferPos + i) != 0)
      }
    }

    private def putBytes(
        columnVector: WritableColumnVector,
        pos: Int,
        bufferPos: Int,
        len: Int): Unit = {
      columnVector.putBytes(pos, len, buffer.array, bufferPos)
    }

    private def putShorts(
        columnVector: WritableColumnVector,
        pos: Int,
        bufferPos: Int,
        len: Int): Unit = {
      columnVector.putShorts(pos, len, buffer.array, bufferPos)
    }

    private def putInts(
        columnVector: WritableColumnVector,
        pos: Int,
        bufferPos: Int,
        len: Int): Unit = {
      columnVector.putInts(pos, len, buffer.array, bufferPos)
    }

    private def putLongs(
        columnVector: WritableColumnVector,
        pos: Int,
        bufferPos: Int,
        len: Int): Unit = {
      columnVector.putLongs(pos, len, buffer.array, bufferPos)
    }

    private def putFloats(
        columnVector: WritableColumnVector,
        pos: Int,
        bufferPos: Int,
        len: Int): Unit = {
      columnVector.putFloats(pos, len, buffer.array, bufferPos)
    }

    private def putDoubles(
        columnVector: WritableColumnVector,
        pos: Int,
        bufferPos: Int,
        len: Int): Unit = {
      columnVector.putDoubles(pos, len, buffer.array, bufferPos)
    }

    private def putByteArray(
        columnVector: WritableColumnVector,
        pos: Int,
        bufferPos: Int,
        len: Int): Unit = {
      columnVector.putByteArray(pos, buffer.array, bufferPos, len)
    }

    private def decompressPrimitive(
        columnVector: WritableColumnVector,
        rowCnt: Int,
        unitSize: Int,
        putFunction: (WritableColumnVector, Int, Int, Int) => Unit): Unit = {
      val nullsBuffer = buffer.duplicate().order(ByteOrder.nativeOrder())
      nullsBuffer.rewind()
      val nullCount = ByteBufferHelper.getInt(nullsBuffer)
      var nextNullIndex = if (nullCount > 0) ByteBufferHelper.getInt(nullsBuffer) else rowCnt
      var valueIndex = 0
      var seenNulls = 0
      var bufferPos = buffer.position()
      while (valueIndex < rowCnt) {
        if (valueIndex != nextNullIndex) {
          val len = nextNullIndex - valueIndex
          assert(len * unitSize.toLong < Int.MaxValue)
          putFunction(columnVector, valueIndex, bufferPos, len)
          bufferPos += len * unitSize
          valueIndex += len
        } else {
          seenNulls += 1
          nextNullIndex =
            if (seenNulls < nullCount) {
              ByteBufferHelper.getInt(nullsBuffer)
            } else {
              rowCnt
            }
          columnVector.putNull(valueIndex)
          valueIndex += 1
        }
      }
    }

    private def decompressString(
        columnVector: WritableColumnVector,
        rowCnt: Int,
        putFunction: (WritableColumnVector, Int, Int, Int) => Unit): Unit = {
      val nullsBuffer = buffer.duplicate().order(ByteOrder.nativeOrder())
      nullsBuffer.rewind()
      val nullCount = ByteBufferHelper.getInt(nullsBuffer)
      var nextNullIndex = if (nullCount > 0) ByteBufferHelper.getInt(nullsBuffer) else rowCnt
      var valueIndex = 0
      var seenNulls = 0
      while (valueIndex < rowCnt) {
        if (valueIndex != nextNullIndex) {
          val len = nextNullIndex - valueIndex
          for (index <- valueIndex until nextNullIndex) {
            val length = buffer.getInt()
            val cursor = buffer.position()
            buffer.position(cursor + length)
            putFunction(columnVector, index, buffer.arrayOffset() + cursor, length)
          }
          valueIndex += len
        } else {
          seenNulls += 1
          nextNullIndex =
            if (seenNulls < nullCount) {
              ByteBufferHelper.getInt(nullsBuffer)
            } else {
              rowCnt
            }
          columnVector.putNull(valueIndex)
          valueIndex += 1
        }
      }
    }

    private def decompressDecimal(
        columnVector: WritableColumnVector,
        rowCnt: Int,
        precision: Int): Unit = {
      if (precision <= Decimal.MAX_INT_DIGITS) decompressPrimitive(columnVector, rowCnt, 4, putInts)
      else if (precision <= Decimal.MAX_LONG_DIGITS) {
        decompressPrimitive(columnVector, rowCnt, 8, putLongs)
      } else {
        decompressString(columnVector, rowCnt, putByteArray)
      }
    }

    override def decompress(columnVector: WritableColumnVector, rowCnt: Int): Unit = {
      columnType.dataType match {
        case _: BooleanType =>
          val unitSize = 1
          decompressPrimitive(columnVector, rowCnt, unitSize, putBooleans)
        case _: ByteType =>
          val unitSize = 1
          decompressPrimitive(columnVector, rowCnt, unitSize, putBytes)
        case _: ShortType =>
          val unitSize = 2
          decompressPrimitive(columnVector, rowCnt, unitSize, putShorts)
        case _: IntegerType =>
          val unitSize = 4
          decompressPrimitive(columnVector, rowCnt, unitSize, putInts)
        case _: LongType =>
          val unitSize = 8
          decompressPrimitive(columnVector, rowCnt, unitSize, putLongs)
        case _: FloatType =>
          val unitSize = 4
          decompressPrimitive(columnVector, rowCnt, unitSize, putFloats)
        case _: DoubleType =>
          val unitSize = 8
          decompressPrimitive(columnVector, rowCnt, unitSize, putDoubles)
        case _: StringType =>
          decompressString(columnVector, rowCnt, putByteArray)
        case d: DecimalType =>
          decompressDecimal(columnVector, rowCnt, d.precision)
      }
    }
  }
}
