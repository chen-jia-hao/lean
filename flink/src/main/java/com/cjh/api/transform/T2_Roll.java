package com.cjh.api.transform;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenjiahao
 * @date 2021/8/18 17:07
 */

public class T2_Roll {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.readTextFile(ClassLoader.getSystemResources("").nextElement().toString() + "sensor.txt");

        DataStream<Sensor> mapStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

//        KeyedStream<Sensor, Tuple> keyedStream = mapStream.keyBy("id");
        KeyedStream<Sensor, String> keyedStream = mapStream.keyBy(Sensor::getId);
        DataStream<Sensor> max = keyedStream.maxBy("value");
        max.print("sum");

        env.execute();
    }
}
