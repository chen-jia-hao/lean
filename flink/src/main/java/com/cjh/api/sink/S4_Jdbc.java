package com.cjh.api.sink;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenjiahao
 * @date 2021/8/20 14:01
 */

public class S4_Jdbc {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.readTextFile(ClassLoader.getSystemResources("").nextElement().toString() + "sensor.txt");

        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        String preSql = "insert into sensor(id, val) values(?,?) on duplicate key update val=?";

        dataStream.addSink(JdbcSink.sink(preSql, (JdbcStatementBuilder<Sensor>) (preparedStatement, sensor) -> {
            preparedStatement.setString(1, sensor.getId());
            preparedStatement.setDouble(2, sensor.getValue());
            preparedStatement.setDouble(3, sensor.getValue());
        }, new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/test?useSSL=true")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root")
                .build()));

        env.execute();
    }
}
