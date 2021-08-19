package com.cjh.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author chenjiahao
 * @date 2021/8/17 14:29
 */

public class WorldCountStream {

    public static void main(String[] args) throws Exception {
        String cls = ClassLoader.getSystemClassLoader().getResource(".").getPath();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//        DataStream<String> ds = env.readTextFile(cls + File.separator + "world.txt");

//        DataStream<String> ds = env.socketTextStream("localhost", 8888);

        ParameterTool tool = ParameterTool.fromArgs(args);
        String host = tool.get("host");
        int port = tool.getInt("port");
        DataStream<String> ds = env.socketTextStream(host, port);

        ds.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (val, out) -> {
                    System.out.println(val);
                    String[] strings = val.split(" ");
                    Arrays.stream(strings).forEach(s -> {
                        out.collect(new Tuple2<String, Integer>(s, 1));
                    });
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1)
                .setParallelism(2)
                .print()
                .setParallelism(1);

        env.execute();
    }
}
