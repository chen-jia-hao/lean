package com.cjh.api.state;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenjiahao
 * @date 2021/8/23 16:59
 */

public class S2_Keyed {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> ds = env.socketTextStream("localhost", 8888);
        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        SingleOutputStreamOperator<Integer> operator = dataStream.keyBy(Sensor::getId)
                .map(new MyRichCount());
        operator.print("rich-count");

        env.execute();
    }

    public static class MyRichCount extends RichMapFunction<Sensor, Integer> {

        private ValueState<Integer> myValStatus;

        private ListState<String> myListStatus;
        private MapState<String, Integer> myMapStatus;
        private ReducingState<Sensor> myReducingStatus;

        @Override
        public void open(Configuration parameters) throws Exception {
            myValStatus = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my-vs", Integer.class));
            myListStatus = getRuntimeContext().getListState(new ListStateDescriptor<>("my-ls", String.class));
            myMapStatus = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("my-ms", String.class, Integer.class));
//            myReducingStatus = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Sensor>())
        }

        @Override
        public Integer map(Sensor value) throws Exception {

            myListStatus.get().forEach(System.out::println);
            myListStatus.add("xxx");

            myMapStatus.put("a", 1);
            System.out.println("myMapStatus.get(\"a\") = " + myMapStatus.get("a"));
            myMapStatus.remove("a");

            Integer count = myValStatus.value();
            if (count == null) {
                count = 0;
            }
            count++;
            myValStatus.update(count);
            return count;
        }
    }
}
