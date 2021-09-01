package com.cjh.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @author chenjiahao
 * @date 2021/8/31 15:01
 */

public class E2 {

    public static void main(String[] args) throws IOException {
        HttpHost httpHost = new HttpHost("192.168.38.128", 9200);
        RestClientBuilder restClientBuilder = RestClient.builder(httpHost);
        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);

        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("address", "mill lane"));
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println("response = " + response);

        client.close();
    }
}
