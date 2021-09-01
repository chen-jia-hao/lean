package com.cjh.es;

import com.cjh.model.Student;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * @author chenjiahao
 * @date 2021/8/31 13:43
 */

public class E1 {
    public static void main(String[] args) throws IOException {
        HttpHost httpHost = new HttpHost("192.168.38.128", 9200);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
        GetIndexRequest bank = new GetIndexRequest("bank");
        boolean exists = client.indices().exists(bank, RequestOptions.DEFAULT);
        System.out.println("exists = " + exists);


        GetResponse student = client.get(new GetRequest().index("student").id("1"), RequestOptions.DEFAULT);
        System.out.println("student = " + student);

        IndexRequest request = new IndexRequest("student");
        request.id("100");
        Student jojo = new Student(100L, "jojo", 17L);
        request.source(new Gson().toJson(jojo), XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println("response = " + response);

        client.close();
    }
}
