package com.cjh.kafka.msg;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author chenjiahao
 * @date 2021/9/6 14:34
 */
@Component
public class TestMsg {

    @KafkaListener(topics = {"t1"}, groupId = "foo")
    public void consumerMsg(String content) {
        System.out.println("content = " + content);
    }
}
