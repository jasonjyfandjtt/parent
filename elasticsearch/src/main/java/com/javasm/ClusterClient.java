package com.javasm;

import com.alibaba.fastjson.JSONObject;
import com.javasm.domin.Hero;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ClusterClient {
    public static void main(String[] args) throws IOException {
        clusterNodeClient();
    }
    private static void clusterNodeClient() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.184.132",9200,"http"),
                new HttpHost("192.168.184.133",9200,"http"),
                new HttpHost("192.168.184.134",9200,"http")));
        Hero hero = new Hero();
        hero.setName("许褚");
        hero.setSkill("马儿快快来送死");

        //组装添加文档的请求
        IndexRequest request = new IndexRequest("hero")//hero 是索引名
                .id("8")//指定id
                .source(JSONObject.toJSONString(hero), XContentType.JSON);//数据内容json格式
        //执行添加文档的命令
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        //返回结果
        System.out.println(response);
        client.close();
    }
}
