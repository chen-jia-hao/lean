package com.cjh.jwt;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;

import java.security.Key;

/**
 * @author chenjiahao
 * @date 2021/7/23 17:33
 */

public class J1 {

    public static void main(String[] args) {
        Key key = Keys.secretKeyFor(SignatureAlgorithm.HS256);

        String jws = Jwts.builder().setSubject("Joe").signWith(key).compact();
        System.out.println("jws = " + jws);
    }
}
