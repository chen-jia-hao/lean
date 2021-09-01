package com.cjh.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chenjiahao
 * @date 2021/8/31 14:51
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Student {

    private Long id;
    private String name;
    private Long age;
}
