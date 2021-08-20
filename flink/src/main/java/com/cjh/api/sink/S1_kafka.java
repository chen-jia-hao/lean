package com.cjh.api.sink;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author chenjiahao
 * @date 2021/8/19 16:53
 */

public class S1_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<String> ds = env.readTextFile(ClassLoader.getSystemResources("").nextElement().toString() + "sensor.txt");

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.187.128:9092");
        DataStream<String> ds = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), props));

        DataStream<String> dataStream = ds.map((MapFunction<String, String>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer<String>("192.168.187.128:9092", "sinktest", new SimpleStringSchema()));

        env.execute();
    }
}
