package org.apache.celeborn.tests.flink;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class SplitReadHelper {
    public static void runSplitRead(StreamExecutionEnvironment env) throws Exception {
        env.fromSequence(0, 1000000)
                .setParallelism(1)
                .disableChaining()
                .map(new MapFunction<Long, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Long key) throws Exception {
                        return new Tuple2<>(key, RandomStringUtils.randomAlphabetic(1000));
                    }
                }).setParallelism(100)
                .disableChaining()
                .map(new MapFunction<Tuple2<Long, String>, String>() {
                    @Override
                    public String map(Tuple2<Long, String> tuple) throws Exception {
                        return tuple.f1;
                    }

                }).setParallelism(10)
                .disableChaining()
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value) throws Exception {

                    }
                }).setParallelism(1);


        env.execute("Shuffle Task");

    }
}
