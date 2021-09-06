package com.cjh.api.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author chenjiahao
 * @date 2021/9/2 14:45
 */

public class S5_kafka_test {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.80.128:9092");

        ParameterTool tool = ParameterTool.fromArgs(args);
        String topic = tool.get("topic");
        DataStream<String> ds = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props));

        ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value.substring(0,2), Integer.parseInt(value.substring(2)));
            }
        }).flatMap(new FlatMapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public void flatMap(Tuple2<String, Integer> value, Collector<String> out) throws Exception {
                if (value.f1 % 1000 == 0) {
                    out.collect("alarm: " + value.f0 + value.f1);
                }
            }
        }).print("alarm");
//        ds.print("kafka");

        env.execute();
    }
}
