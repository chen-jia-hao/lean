package com.cjh.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.File;
import java.util.Arrays;

/**
 * @author chenjiahao
 * @date 2021/8/17 13:55
 */

public class WorldCount {

    public static void main(String[] args) throws Exception {
        String cls = ClassLoader.getSystemClassLoader().getResource(".").getPath();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> source = env.readTextFile(cls + File.separator + "world.txt");
        source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (val, out) -> {
                    System.out.println(val);
                    String[] strings = val.split(" ");
                    Arrays.stream(strings).forEach(s -> {
                        out.collect(new Tuple2<String, Integer>(s, 1));
                    });
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .groupBy(0)
                .sum(1)
                .print();
    }
}
