package com.cjh.api.source;

import com.cjh.model.Sensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author chenjiahao
 * @date 2021/8/18 16:08
 */

public class S4_Source {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Sensor> ds = env.addSource(new SensorSource());

        ds.print("sensor");

        env.execute();
    }

    public static class SensorSource implements SourceFunction<Sensor> {

        private boolean running = true;

        @Override
        public void run(SourceContext<Sensor> ctx) throws Exception {
            Map<String, Double> map = new HashMap<>(16);
            Random random = new Random();
            for (int i = 1; i <= 5; i++) {
                map.put("sensor_" + i, 60 + random.nextGaussian() * 20);
            }

            while (running) {
                map.forEach((k, v) -> {
                    ctx.collect(new Sensor(k, System.currentTimeMillis(), v));
                });
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
