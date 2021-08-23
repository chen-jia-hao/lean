package com.cjh.api.window;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author chenjiahao
 * @date 2021/8/23 11:04
 */

public class W5_Event {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        /**
         * 设置从此环境创建的所有流的时间特征，例如处理时间、事件时间或摄取时间。
         * 如果您将特征设置为 EventTime 的 IngestionTime，这将设置默认的水印更新间隔为 200 毫秒。
         * 如果这不适用于您的应用程序，您应该使用ExecutionConfig.setAutoWatermarkInterval(long)更改它。
         * 已弃用
         * 在 Flink 1.12 中，默认的流时间特性已更改为TimeCharacteristic.EventTime ，
         * 因此您不再需要调用此方法来启用事件时间支持。 显式使用处理时间窗口和计时器在事件时间模式下工作。
         * 如果您需要禁用水印，请使用ExecutionConfig.setAutoWatermarkInterval(long) 。
         * 如果您使用TimeCharacteristic.IngestionTime ，请手动设置适当的WatermarkStrategy 。
         * 如果您使用通用的“时间窗口”操作
         * （例如org.apache.flink.streaming.api.datastream.KeyedStream.timeWindow(org.apache.flink.streaming.api.windowing.time.Time) ，
         * 它会根据时间特性，请使用明确指定处理时间或事件时间的等效操作。
         * 参数：
         * 特征 – 时间特征。
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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

        SingleOutputStreamOperator<Sensor> min = operator.keyBy(Sensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .minBy("value");

        min.print("minVal");

        env.execute();
    }
}
