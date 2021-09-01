package com.cjh.elk.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.*;

/**
 * @author chenjiahao
 * @date 2021/9/1 11:10
 */
@Slf4j
@RestController
public class TestController {

    @GetMapping("/foo")
    public Map<String, Object> foo() {
        Map<String, Object> map = new HashMap<>(16);
        map.put("code", 0);
        map.put("msg", "foo test");
        map.put("data", new Date());
        ServletRequestAttributes requestAttributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();

        log.info("headers {}", requestAttributes.getRequest().getHeader("user-agent"));
        return map;
    }
}
