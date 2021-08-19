package com.cjh.api.transform;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenjiahao
 * @date 2021/8/19 13:53
 */

public class T3_Reduce {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.readTextFile(ClassLoader.getSystemResources("").nextElement().toString() + "sensor.txt");

        DataStream<Sensor> mapStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        KeyedStream<Sensor, String> keyedStream = mapStream.keyBy(Sensor::getId);
        SingleOutputStreamOperator<Sensor> reduce = keyedStream.reduce((ReduceFunction<Sensor>) (sensor, t1)
                -> new Sensor(sensor.getId(), t1.getTimestamp(), Math.max(sensor.getValue(), t1.getValue())));
        reduce.print("sum");

        env.execute();
    }
}
