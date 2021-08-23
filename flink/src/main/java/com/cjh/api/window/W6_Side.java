package com.cjh.api.window;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author chenjiahao
 * @date 2021/8/23 14:54
 */

public class W6_Side {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        env.getConfig().setAutoWatermarkInterval(100);

        DataStream<String> ds = env.socketTextStream("localhost", 8888);
        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        WatermarkStrategy<Sensor> objectWatermarkStrategy = WatermarkStrategy
                .forBoundedOutOfOrderness(Duration.ofSeconds(2));

        SingleOutputStreamOperator<Sensor> operator = dataStream
                .assignTimestampsAndWatermarks(objectWatermarkStrategy
                        .withTimestampAssigner(new SerializableTimestampAssigner<Sensor>() {
                            @Override
                            public long extractTimestamp(Sensor element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );

        OutputTag<Sensor> tag = new OutputTag<Sensor>("late"){};

        SingleOutputStreamOperator<Sensor> min = operator.keyBy(Sensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(tag)
                .minBy("value");

        min.print("minVal");

        min.getSideOutput(tag).print("lateMin");

        env.execute();
    }
}
