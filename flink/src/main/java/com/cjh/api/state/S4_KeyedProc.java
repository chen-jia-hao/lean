package com.cjh.api.state;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author chenjiahao
 * @date 2021/8/24 14:52
 */

public class S4_KeyedProc {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> ds = env.socketTextStream("localhost", 8888);
        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        dataStream.keyBy(Sensor::getId)
                .process(new MyKeyedProc())
                .print("proc");

        env.execute();
    }

    public static class MyKeyedProc extends KeyedProcessFunction<String, Sensor, Integer> {

        private ValueState<Long> timerStatus;

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Sensor, Integer>.OnTimerContext ctx, Collector<Integer> out) throws Exception {

            System.out.println(timestamp + " onTimer trigger...");
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timeDomain();
            ctx.timerService();
//            out.collect();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            timerStatus =  getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
        }

        @Override
        public void close() throws Exception {
            timerStatus.clear();
        }

        @Override
        public void processElement(Sensor value, KeyedProcessFunction<String, Sensor, Integer>.Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

//            ctx.output();
            System.out.println("ctx.timestamp() = " + ctx.timestamp() + ", " + ctx.getCurrentKey());
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
            ctx.timerService().registerEventTimeTimer(value.getTimestamp() + 3000L);
//            ctx.timerService().deleteProcessingTimeTimer(value.getTimestamp() + 5000L);
            ctx.timerService().deleteEventTimeTimer(value.getTimestamp() + 3000L);

            timerStatus.update(value.getTimestamp() + 5000L);
        }
    }
}
