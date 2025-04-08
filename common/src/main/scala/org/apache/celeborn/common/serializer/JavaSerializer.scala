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

package org.apache.celeborn.common.serializer

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.network.protocol.{SerdeVersion, TransportMessage}
import org.apache.celeborn.common.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}

private[celeborn] class JavaSerializationStream(
    out: OutputStream,
    counterReset: Int,
    extraDebugInfo: Boolean)
  extends SerializationStream {
  private val objOut = new ObjectOutputStream(out)
  private var counter = 0

  /**
   * Calling reset to avoid memory leak:
   * http://stackoverflow.com/questions/1281549/memory-leak-traps-in-the-java-standard-api
   * But only call it every 100th time to avoid bloated serialization streams (when
   * the stream 'resets' object class descriptions have to be re-written)
   */
  def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      objOut.writeObject(t)
    } catch {
      case e: NotSerializableException if extraDebugInfo =>
        throw SerializationDebugger.improveException(t, e)
    }
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    }
    this
  }

  def flush() { objOut.flush() }
  def close() { objOut.close() }
}

private[celeborn] class JavaDeserializationStream(in: InputStream, loader: ClassLoader)
  extends DeserializationStream {

  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          JavaDeserializationStream.primitiveMappings.getOrElse(desc.getName, throw e)
      }
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]
  def close() { objIn.close() }
}

private object JavaDeserializationStream {
  val primitiveMappings = Map[String, Class[_]](
    "boolean" -> classOf[Boolean],
    "byte" -> classOf[Byte],
    "char" -> classOf[Char],
    "short" -> classOf[Short],
    "int" -> classOf[Int],
    "long" -> classOf[Long],
    "float" -> classOf[Float],
    "double" -> classOf[Double],
    "void" -> classOf[Void])
}

private[celeborn] class JavaSerializerInstance(
    counterReset: Int,
    extraDebugInfo: Boolean,
    defaultClassLoader: ClassLoader)
  extends SerializerInstance {

  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val msg = Utils.toTransportMessage(t)
    msg match {
      case transMsg: TransportMessage =>
        // Check if the msg is a TransportMessage with language-agnostic V2 serdeVersion.
        // If so, write the marker and the body explicitly.
        if (transMsg.getSerdeVersion == SerdeVersion.V2) {
          val out = new DataOutputStream(bos)
          out.writeByte(SerdeVersion.V2.getMarker)
          out.write(transMsg.toByteBuffer.array)
          out.close()
          return bos.toByteBuffer
        }
      case _ =>
    }
    val out = serializeStream(bos)
    out.writeObject(Utils.toTransportMessage(t))
    out.close()
    bos.toByteBuffer
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    bytes.mark
    val serdeVersion = bytes.get
    // If the serdeVersion byte is V2, deserialize directly.
    if (serdeVersion == SerdeVersion.V2.getMarker) {
      return Utils.fromTransportMessage(
        TransportMessage.fromByteBuffer(bytes, SerdeVersion.V2)).asInstanceOf[T]
    }
    bytes.reset
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    Utils.fromTransportMessage(in.readObject()).asInstanceOf[T]
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    bytes.mark
    val serdeVersion = bytes.get
    // If the serdeVersion byte is V2, deserialize directly.
    if (serdeVersion == SerdeVersion.V2.getMarker) {
      return Utils.fromTransportMessage(
        TransportMessage.fromByteBuffer(bytes, SerdeVersion.V2)).asInstanceOf[T]
    }
    bytes.reset
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis, loader)
    Utils.fromTransportMessage(in.readObject()).asInstanceOf[T]
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s, counterReset, extraDebugInfo)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, defaultClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }
}

/**
 * :: DeveloperApi ::
 * A Spark serializer that uses Java's built-in serialization.
 *
 * @note This serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
 *
 * @note Java Object Serialization Specification:
 * https://docs.oracle.com/javase/8/docs/platform/serialization/spec/protocol.html
 */
class JavaSerializer(conf: CelebornConf) extends Serializer with Externalizable {
  private var counterReset = conf.getInt("spark.serializer.objectStreamReset", 100)
  private var extraDebugInfo = conf.getBoolean("spark.serializer.extraDebugInfo", true)

  protected def this() = this(new CelebornConf()) // For deserialization only

  override def newInstance(): SerializerInstance = {
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)
    new JavaSerializerInstance(counterReset, extraDebugInfo, classLoader)
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeInt(counterReset)
    out.writeBoolean(extraDebugInfo)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    counterReset = in.readInt()
    extraDebugInfo = in.readBoolean()
  }
}
