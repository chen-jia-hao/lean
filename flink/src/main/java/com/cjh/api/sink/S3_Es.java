package com.cjh.api.sink;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenjiahao
 * @date 2021/8/20 10:40
 */

public class S3_Es {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.readTextFile(ClassLoader.getSystemResources("").nextElement().toString() + "sensor.txt");


        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("192.168.19.128", 9200));

        dataStream.addSink(new ElasticsearchSink.Builder<Sensor>(hosts, new ElasticsearchSinkFunction<Sensor>() {
            @Override
            public void process(Sensor sensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                Map<String, String> src = new HashMap<>(16);
                src.put("id", sensor.getId());
                src.put("time", sensor.getTimestamp().toString());
                src.put("val", sensor.getValue().toString());

                IndexRequest indexRequest = Requests.indexRequest()
                        .index("sensor")
                        .type("reading")
                        .source(src);
                requestIndexer.add(indexRequest);
            }
        }).build());

        env.execute();
    }
}
