package com.cjh.api.window;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author chenjiahao
 * @date 2021/8/23 9:59
 */

public class W4_Lateness {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.socketTextStream("localhost", 8888);
        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        OutputTag<Sensor> tag = new OutputTag<>("late");

        SingleOutputStreamOperator<Sensor> sum = dataStream.keyBy(Sensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//                        .trigger()
//        .evictor()
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(tag)
                .sum("value");
        sum.getSideOutput(tag).print("side late");

        env.execute();
    }
}
