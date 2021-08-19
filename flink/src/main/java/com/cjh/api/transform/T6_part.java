package com.cjh.api.transform;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenjiahao
 * @date 2021/8/19 16:00
 */

public class T6_part {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.readTextFile(ClassLoader.getSystemResources("").nextElement().toString() + "sensor.txt");

        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        DataStream<Sensor> shuffleStream = dataStream.shuffle();
        dataStream.print("ds");
        shuffleStream.print("shuffle");
        shuffleStream.keyBy(Sensor::getId).print("key");
        dataStream.global().print("global");

        env.execute();
    }
}
