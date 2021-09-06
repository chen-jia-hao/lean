package com.cjh.test;

import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import okhttp3.*;
import org.junit.Test;

import java.io.IOException;

/**
 * @author chenjiahao
 * @date 2021/9/3 14:34
 */

public class T1 {

    @Test
    public void test02 () {
        String url = "http://10.101.196.130:10086/api/getRTDataByBatch";
        OkHttpClient httpClient = new OkHttpClient();
        MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded");
        RequestBody body = RequestBody.create("tagname=test.a0,test.a1", mediaType);
        Request request = new Request.Builder()
                .post(body)
                .url(url)
                .build();
        try {
            Response response = httpClient.newCall(request).execute();
            if (response.isSuccessful()) {
                System.out.println("response.body() = " + response.body().string());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test () {
        String url = "http://10.101.196.130:10086/api/getRTDataByBatch";

        HttpResponse response = HttpUtil.createPost(url)
                .body("tagname=test.a0", "application/x-www-form-urlencoded")
                .execute();
        System.out.println("response.body() = " + response.body());
    }
}
