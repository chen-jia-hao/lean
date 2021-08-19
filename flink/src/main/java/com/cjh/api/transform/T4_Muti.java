package com.cjh.api.transform;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author chenjiahao
 * @date 2021/8/19 14:05
 */

public class T4_Muti {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.readTextFile(ClassLoader.getSystemResources("").nextElement().toString() + "sensor.txt");

        DataStream<Sensor> mapStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        final OutputTag<Sensor> high = new OutputTag<Sensor>("high", TypeInformation.of(Sensor.class));
        final OutputTag<Sensor> low = new OutputTag<>("low", TypeInformation.of(Sensor.class));

        SingleOutputStreamOperator<Object> mainStream = mapStream.process(new ProcessFunction<Sensor, Object>() {
            @Override
            public void processElement(Sensor value, ProcessFunction<Sensor, Object>.Context ctx, Collector<Object> out) throws Exception {
                out.collect(value);
                if (value.getValue() > 36.5) {
                    ctx.output(high, value);
                } else {
                    ctx.output(low, value);
                }

            }
        });

        DataStream<Sensor> highStream = mainStream.getSideOutput(high);
        DataStream<Sensor> lowStream = mainStream.getSideOutput(low);
        highStream.print("high");
        lowStream.print("low");
        mainStream.print("main");

        DataStream<Tuple2<String, Double>> alarmStream = highStream.map(new MapFunction<Sensor, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Sensor sensor) throws Exception {
                return new Tuple2<>(sensor.getId(), sensor.getValue());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, Sensor> connectedStreams = alarmStream.connect(lowStream);
        SingleOutputStreamOperator<Tuple3<String, Double, String>> streamOperator = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, Sensor, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warn");
            }

            @Override
            public Tuple3<String, Double, String> map2(Sensor value) throws Exception {
                return new Tuple3<>(value.getId(), value.getValue(), "normal");
            }
        });
        streamOperator.print("ret");


        env.execute();
    }
}
