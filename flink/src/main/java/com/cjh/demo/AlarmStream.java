package com.cjh.demo;

import com.cjh.model.TagData;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author chenjiahao
 * @date 2021/8/18 9:39
 */

public class AlarmStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.socketTextStream("localhost", 8888);
        DataStream<TagData> map = ds.map(x -> {
            String[] strings = x.split(",");
            return new TagData(Long.parseLong(strings[0]), strings[1], Double.parseDouble(strings[2]));
        });
        DataStream<TagData> streamOperator = map.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<TagData>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(TagData element) {
                return element.getId() * 1000;
            }
        });
        DataStream<Object> process = streamOperator.keyBy("name")
                .process(new KeyedProcessFunction<Tuple, TagData, Object>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                    }

                    ValueState<Double> last = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last", Double.class));
                    ValueState<Long> timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));

                    @Override
                    public void processElement(TagData value, KeyedProcessFunction<Tuple, TagData, Object>.Context ctx, Collector<Object> out) throws Exception {
                        Double pre = last.value();
                        Long cur = timer.value();
                        last.update(value.getValue());

                        if (value.getValue() > pre && cur == 0) {
                            long tms = ctx.timerService().currentProcessingTime() + 10000;
                            ctx.timerService().registerEventTimeTimer(tms);
                            timer.update(tms);
                        } else if (value.getValue() < pre && pre == 0) {
                            ctx.timerService().deleteProcessingTimeTimer(cur);
                            timer.clear();
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Tuple, TagData, Object>.OnTimerContext ctx, Collector<Object> out) throws Exception {
                        out.collect("tag: [" + ctx.getCurrentKey() + "] trigger...");
                    }
                });

        map.print("tag data");
        process.print("process func");

        env.execute("test job");
    }
}

class tagProcess extends KeyedProcessFunction<String, TagData, String> {

    @Override
    public void processElement(TagData value, KeyedProcessFunction<String, TagData, String>.Context ctx, Collector<String> out) throws Exception {

    }
}

