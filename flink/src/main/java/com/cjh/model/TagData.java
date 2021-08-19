package com.cjh.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chenjiahao
 * @date 2021/8/18 10:01
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TagData {

    private Long id;
    private String name;
    private Double value;
}
