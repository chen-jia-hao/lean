package com.cjh.api.window;

import com.cjh.model.Sensor;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author chenjiahao
 * @date 2021/8/21 23:22
 */

public class W2_Full {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.socketTextStream("localhost", 8888);
        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> apply = dataStream.keyBy(Sensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .apply(new WindowFunction<Sensor, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<Sensor> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        int size = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3<>(s, timeWindow.getEnd(), size));
                    }
                });

        apply.print();
        env.execute();
    }
}
