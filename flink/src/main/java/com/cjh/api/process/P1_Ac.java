package com.cjh.api.process;

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
 * @date 2021/8/24 15:15
 */

public class P1_Ac {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> ds = env.socketTextStream("localhost", 8888);
        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        dataStream.keyBy(Sensor::getId)
                .process(new MyAcProc())
                .print("ac");

        env.execute();
    }

    public static class MyAcProc extends KeyedProcessFunction<String, Sensor, Integer> {

        private ValueState<Long> timerStatus;
        private ValueState<Double> lastTemp;

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Sensor, Integer>.OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println("10s 连续上升 " + timestamp + ", " + ctx.getCurrentKey());
            timerStatus.clear();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            timerStatus =  getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
            lastTemp =  getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
        }

        @Override
        public void close() throws Exception {
            timerStatus.clear();
            lastTemp.clear();
        }

        @Override
        public void processElement(Sensor value, KeyedProcessFunction<String, Sensor, Integer>.Context ctx, Collector<Integer> out) throws Exception {
            Double last = lastTemp.value();
            Long timerTs = timerStatus.value();
//            System.out.println("timerTs = " + timerTs);
            if (last != null) {
                if (last < value.getValue() && timerTs == null) {
                    long ts = ctx.timerService().currentProcessingTime() + 10000L;
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    timerStatus.update(ts);
                }
                if (last >= value.getValue() && timerTs != null) {
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                }
            }
            lastTemp.update(value.getValue());
        }
    }
}

