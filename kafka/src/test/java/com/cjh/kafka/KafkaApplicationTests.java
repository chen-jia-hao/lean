package com.cjh.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class KafkaApplicationTests {

    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    void contextLoads() throws InterruptedException {
        for (int j = 0; j < 1000; j++) {
            Instant start = Instant.now();
            for (int i = 0; i < 200000; i++) {
                kafkaTemplate.send("t1", "hi" + i);
            }
            System.out.println(Duration.between(start, Instant.now()).toMillis() + "ms " + j);
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
