package com.cjh.api.process;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author chenjiahao
 * @date 2021/8/24 15:54
 */

public class P2_SideOut {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> ds = env.socketTextStream("localhost", 8888);
        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        OutputTag<Sensor> low = new OutputTag<Sensor>("low"){};
        SingleOutputStreamOperator<Sensor> high = dataStream.process(new ProcessFunction<Sensor, Sensor>() {
            @Override
            public void processElement(Sensor value, ProcessFunction<Sensor, Sensor>.Context ctx, Collector<Sensor> out) throws Exception {
                if (value.getValue() > 36.5) {
                    out.collect(value);
                } else {
                    ctx.output(low, value);
                }
            }
        });

        high.print("high");
        high.getSideOutput(low).print("low");

        env.execute();
    }
}
