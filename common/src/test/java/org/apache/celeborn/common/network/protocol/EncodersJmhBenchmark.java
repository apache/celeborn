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

package org.apache.celeborn.common.network.protocol;

import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmark for celeborn's {@link Encoders} serialization primitives.
 *
 * <p>{@code Encoders.Strings/IntArrays/StringArrays} are the canonical write/read primitives used
 * by every concrete transport {@link Message} (e.g. {@link PushMergedData}, {@link PushData}). They
 * are the micro-hot-path of the network serialization layer, so this benchmark mirrors the Kafka
 * request/response serialization benchmarks (KAFKA-8106 / KAFKA-14633 reduce buffer allocation &
 * data copy) but measures celeborn's own primitives rather than Kafka's protocol structs.
 *
 * <p>The {@code decode} paths allocate fresh {@code byte[]} / {@code String} objects each call, so
 * the benchmark also captures allocation pressure; {@code encode} reuses a single destination
 * buffer to isolate that from the encoding cost.
 *
 * <p>To run:
 *
 * <pre>{@code
 * build/mvn -pl common -am test-compile
 * build/mvn -pl common exec:java \
 *   -Dexec.mainClass=org.apache.celeborn.common.network.protocol.EncodersJmhBenchmark \
 *   -Dexec.classpathScope=test \
 *   -Dexec.args="-f 0 -wi 1 -i 1"
 * }</pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class EncodersJmhBenchmark {

  @Param({"16", "256"})
  private int stringBytes;

  @Param({"8", "128"})
  private int arrayLen;

  @Param({"42"})
  private int seed;

  private String sampleString;
  private int[] sampleIntArray;
  private String[] sampleStringArray;
  private ByteBuf encodeBuf;

  @Setup
  public void setup() {
    SplittableRandom random = new SplittableRandom(seed);
    sampleString = randomString(random, stringBytes);
    sampleIntArray = new int[arrayLen];
    for (int i = 0; i < arrayLen; i++) {
      sampleIntArray[i] = random.nextInt();
    }
    sampleStringArray = new String[arrayLen];
    for (int i = 0; i < arrayLen; i++) {
      sampleStringArray[i] = randomString(random, stringBytes);
    }
    encodeBuf = Unpooled.buffer(Encoders.StringArrays.encodedLength(sampleStringArray));
  }

  private static String randomString(SplittableRandom random, int byteLen) {
    byte[] bytes = new byte[byteLen];
    for (int i = 0; i < byteLen; i++) {
      // printable ascii range to keep UTF-8 length == byte length
      bytes[i] = (byte) random.nextInt('a', 'z' + 1);
    }
    return new String(bytes);
  }

  @Benchmark
  public void encodeString(Blackhole blackhole) {
    encodeBuf.clear();
    Encoders.Strings.encode(encodeBuf, sampleString);
    blackhole.consume(encodeBuf);
  }

  @Benchmark
  public String decodeString() {
    ByteBuf buf = Unpooled.buffer(Encoders.Strings.encodedLength(sampleString));
    Encoders.Strings.encode(buf, sampleString);
    return Encoders.Strings.decode(buf);
  }

  @Benchmark
  public void encodeIntArray(Blackhole blackhole) {
    encodeBuf.clear();
    Encoders.IntArrays.encode(encodeBuf, sampleIntArray);
    blackhole.consume(encodeBuf);
  }

  @Benchmark
  public int[] decodeIntArray() {
    ByteBuf buf = Unpooled.buffer(Encoders.IntArrays.encodedLength(sampleIntArray));
    Encoders.IntArrays.encode(buf, sampleIntArray);
    return Encoders.IntArrays.decode(buf);
  }

  @Benchmark
  public void encodeStringArray(Blackhole blackhole) {
    encodeBuf.clear();
    Encoders.StringArrays.encode(encodeBuf, sampleStringArray);
    blackhole.consume(encodeBuf);
  }

  @Benchmark
  public String[] decodeStringArray() {
    ByteBuf buf = Unpooled.buffer(Encoders.StringArrays.encodedLength(sampleStringArray));
    Encoders.StringArrays.encode(buf, sampleStringArray);
    return Encoders.StringArrays.decode(buf);
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
