package com.cjh.api.source;

import com.cjh.model.Sensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author chenjiahao
 * @date 2021/8/18 14:00
 */

public class S1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Sensor> ds = env.fromCollection(Arrays.asList(
                new Sensor("sensor_1", 1629267025569L, 33.4),
                new Sensor("sensor_2", 1629267025569L, 35.4),
                new Sensor("sensor_3", 1629267025569L, 32.4),
                new Sensor("sensor_7", 1629267025569L, 23.4),
                new Sensor("sensor_9", 1629267025569L, 25.6)
        ));
        DataStream<Integer> intDS = env.fromElements(1, 2, 3, 4, 6, 8);

        DataStream<Long> longDS = env.fromSequence(18, 30);

        ds.print("sensor").setParallelism(1);
        intDS.print("int");
        longDS.print("long");

        env.execute();
    }
}
