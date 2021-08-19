package com.cjh.api.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author chenjiahao
 * @date 2021/8/18 16:39
 */

public class T1_Base {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.readTextFile(ClassLoader.getSystemResources("").nextElement().toString() + "sensor.txt");

        DataStream<Integer> mapStream = ds.map((MapFunction<String, Integer>) String::length);
        DataStream<String> flatMapStream = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] strings = s.split(",");
                Arrays.stream(strings).forEach(collector::collect);
            }
        });

        DataStream<String> filterStream = ds.filter((FilterFunction<String>) s -> s.startsWith("sensor_1"));

        mapStream.print("map");
        flatMapStream.print("flatmap");
        filterStream.print("filter");

        env.execute();
    }
}
