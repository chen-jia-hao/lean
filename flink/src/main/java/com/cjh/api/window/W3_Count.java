package com.cjh.api.window;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenjiahao
 * @date 2021/8/22 16:20
 */

public class W3_Count {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.socketTextStream("localhost", 8888);
        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        SingleOutputStreamOperator<Double> aggregate = dataStream.keyBy(Sensor::getId)
                .countWindow(10, 2)
                .aggregate(new AggregateFunction<Sensor, Tuple2<Double, Integer>, Double>() {
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0d, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(Sensor sensor, Tuple2<Double, Integer> doubleIntegerTuple2) {
                        return new Tuple2<>(doubleIntegerTuple2.f0 + sensor.getValue(), doubleIntegerTuple2.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> doubleIntegerTuple2) {
                        return doubleIntegerTuple2.f0 / doubleIntegerTuple2.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> doubleIntegerTuple2, Tuple2<Double, Integer> acc1) {
                        return new Tuple2<>(doubleIntegerTuple2.f0 + acc1.f0, doubleIntegerTuple2.f1 + acc1.f1);
                    }
                });


        aggregate.print();
        env.execute();
    }
}
