package com.cjh.api.transform;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenjiahao
 * @date 2021/8/19 15:21
 */

public class T5_Rich {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.readTextFile(ClassLoader.getSystemResources("").nextElement().toString() + "sensor.txt");

        DataStream<Sensor> mapStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = mapStream.map(new MyMap());
        operator.print("rich");

        env.execute();
    }

    public static class MyMap extends RichMapFunction<Sensor, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(Sensor sensor) throws Exception {
            return new Tuple2<>(sensor.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close");
        }
    }
}
