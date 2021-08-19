package com.cjh.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chenjiahao
 * @date 2021/8/18 14:00
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Sensor {

    private String id;
    private Long timestamp;
    private Double value;
}
