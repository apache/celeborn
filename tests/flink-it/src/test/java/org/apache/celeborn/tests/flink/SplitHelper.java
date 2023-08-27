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

package org.apache.celeborn.tests.flink;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.junit.Assert;

public class SplitHelper {
  private static final int NUM_WORDS = 10000;

  private static final Long WORD_COUNT = 200L;

  public static void runSplitRead(StreamExecutionEnvironment env) throws Exception {
    DataStream<Tuple2<String, Long>> words =
        env.fromSequence(0, NUM_WORDS)
            .map(
                new MapFunction<Long, String>() {
                  @Override
                  public String map(Long index) throws Exception {
                    return index + "_" + RandomStringUtils.randomAlphabetic(10);
                  }
                })
            .flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                  @Override
                  public void flatMap(String s, Collector<Tuple2<String, Long>> collector)
                      throws Exception {
                    for (int i = 0; i < WORD_COUNT; ++i) {
                      collector.collect(new Tuple2<>(s, 1L));
                    }
                  }
                });
    words
        .keyBy(value -> value.f0)
        .sum(1)
        .map((MapFunction<Tuple2<String, Long>, Long>) wordCount -> wordCount.f1)
        .addSink(
            new SinkFunction<Long>() {
              @Override
              public void invoke(Long value, Context context) throws Exception {
                Assert.assertEquals(value, WORD_COUNT);
                //                      Thread.sleep(30 * 1000);
              }
            });
  }
}
