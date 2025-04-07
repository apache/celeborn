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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountHelper {

  private static final int NUM_WORDS = 20;

  private static final int WORD_COUNT = 200;

  public static void execute(StreamExecutionEnvironment env, int parallelism) {
    DataStream<Tuple2<String, Long>> words =
        env.fromSequence(0, NUM_WORDS)
            .broadcast()
            .map(new WordsMapper())
            .flatMap(new WordsFlatMapper(WORD_COUNT));
    words
        .keyBy(value -> value.f0)
        .sum(1)
        .map((MapFunction<Tuple2<String, Long>, Long>) wordCount -> wordCount.f1)
        .sinkTo(
            (Sink<Long>)
                writerInitContext ->
                    new SinkWriter<Long>() {
                      @Override
                      public void write(Long value, Context context)
                          throws IOException, InterruptedException {
                        assertEquals(parallelism * WORD_COUNT, value.longValue());
                      }

                      @Override
                      public void flush(boolean endOfInput)
                          throws IOException, InterruptedException {}

                      @Override
                      public void close() throws Exception {}
                    });
  }

  private static class WordsMapper implements MapFunction<Long, String> {

    private static final long serialVersionUID = -896627105414186948L;

    private static final String WORD_SUFFIX_1K = getWordSuffix1k();

    private static String getWordSuffix1k() {
      StringBuilder builder = new StringBuilder();
      builder.append("-");
      for (int i = 0; i < 1024; ++i) {
        builder.append("0");
      }
      return builder.toString();
    }

    @Override
    public String map(Long value) {
      return "WORD-" + value + WORD_SUFFIX_1K;
    }
  }

  private static class WordsFlatMapper implements FlatMapFunction<String, Tuple2<String, Long>> {

    private static final long serialVersionUID = 7873046672795114433L;

    private final int wordsCount;

    public WordsFlatMapper(int wordsCount) {
      this.wordsCount = wordsCount;
    }

    @Override
    public void flatMap(String word, Collector<Tuple2<String, Long>> collector) {
      for (int i = 0; i < wordsCount; ++i) {
        collector.collect(new Tuple2<>(word, 1L));
      }
    }
  }
}
