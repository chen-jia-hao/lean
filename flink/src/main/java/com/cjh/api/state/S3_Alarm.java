package com.cjh.api.state;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chenjiahao
 * @date 2021/8/24 13:57
 */

public class S3_Alarm {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> ds = env.socketTextStream("localhost", 8888);
        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> operator = dataStream.keyBy(Sensor::getId)
                .flatMap(new AlarmFunc(10.0));

        operator.print("alarm");
        env.execute();
    }

    public static class AlarmFunc extends RichFlatMapFunction<Sensor, Tuple3<String, Double, Double>> {

        private Double threshold;
        private ValueState<Double> last;

        public AlarmFunc(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            last = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }

        @Override
        public void flatMap(Sensor value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double value1 = last.value();
            if (value1 != null) {
                if (Math.abs(value1 - value.getValue()) >= threshold) {
                    out.collect(new Tuple3<>(value.getId(), value1, value.getValue()));
                }
            }
            last.update(value.getValue());
        }

        @Override
        public void close() throws Exception {
            last.clear();
        }
    }
}
