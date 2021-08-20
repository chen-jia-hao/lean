package com.cjh.api.sink;

import com.cjh.model.Sensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author chenjiahao
 * @date 2021/8/20 9:47
 */

public class S2_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.readTextFile(ClassLoader.getSystemResources("").nextElement().toString() + "sensor.txt");

        DataStream<Sensor> dataStream = ds.map((MapFunction<String, Sensor>) s -> {
            String[] strings = s.split(",");
            return new Sensor(strings[0], Long.parseLong(strings[1]), Double.parseDouble(strings[2]));
        });

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("")
                .setPort(6379)
                .build();

        dataStream.addSink(new RedisSink<>(config, new RedisMapper<Sensor>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
            }

            @Override
            public String getKeyFromData(Sensor sensor) {
                return sensor.getId();
            }

            @Override
            public String getValueFromData(Sensor sensor) {
                return sensor.getValue().toString();
            }
        }));

        env.execute();
    }
}
