package com.cjh.api.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author chenjiahao
 * @date 2021/8/18 14:41
 */

public class S3_kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.5.128:9092");
        DataStream<String> ds = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), props));

        ds.print("kafka");

        env.execute();
    }
}
