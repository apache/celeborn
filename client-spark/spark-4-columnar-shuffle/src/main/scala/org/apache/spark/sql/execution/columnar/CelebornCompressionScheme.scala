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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.execution.vectorized.WritableColumnVector

trait Encoder[T <: PhysicalDataType] {
  def gatherCompressibilityStats(row: InternalRow, ordinal: Int): Unit = {}

  def compressedSize: Int

  def uncompressedSize: Int

  def compressionRatio: Double = {
    if (uncompressedSize > 0) compressedSize.toDouble / uncompressedSize else 1.0
  }

  def compress(from: ByteBuffer, to: ByteBuffer): ByteBuffer
}

trait Decoder[T <: PhysicalDataType] {
  def next(row: InternalRow, ordinal: Int): Unit

  def hasNext: Boolean

  def decompress(columnVector: WritableColumnVector, capacity: Int): Unit
}

trait CelebornCompressionScheme {
  def typeId: Int

  def supports(columnType: CelebornColumnType[_]): Boolean

  def encoder[T <: PhysicalDataType](columnType: NativeCelebornColumnType[T]): Encoder[T]

  def decoder[T <: PhysicalDataType](
      buffer: ByteBuffer,
      columnType: NativeCelebornColumnType[T]): Decoder[T]
}

trait WithCelebornCompressionSchemes {
  def schemes: Seq[CelebornCompressionScheme]
}

trait AllCelebornCompressionSchemes extends WithCelebornCompressionSchemes {
  override val schemes: Seq[CelebornCompressionScheme] = CelebornCompressionScheme.all
}

object CelebornCompressionScheme {
  val all: Seq[CelebornCompressionScheme] =
    Seq(CelebornPassThrough, CelebornDictionaryEncoding)

  private val typeIdToScheme = all.map(scheme => scheme.typeId -> scheme).toMap

  def apply(typeId: Int): CelebornCompressionScheme = {
    typeIdToScheme.getOrElse(
      typeId,
      throw new UnsupportedOperationException(s"Unrecognized compression scheme type ID: $typeId"))
  }
}
