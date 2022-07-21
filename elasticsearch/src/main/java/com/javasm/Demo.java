package com.javasm;

import com.alibaba.fastjson.JSONObject;
import com.javasm.domin.Hero;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class Demo {
    public static void main(String[] args) throws Exception {
        createDoc();
        updateDoc();
        getById();
        deleteDoc();


    }

    private static void deleteDoc() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.137.35", 9200, "http")));
        //创建请求对象，两个参数，分别是：索引名、文档ID
        DeleteRequest request = new DeleteRequest("hero", "4");
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(response);
        client.close();
    }

    private static void getById() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.137.35", 9200, "http")));

        //创建请求对象，两个参数，分别是：索引名、文档ID
        GetRequest request = new GetRequest("hero", "2");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println(response);
        System.out.println(response.getSourceAsMap());
        client.close();
    }

    private static void updateDoc() throws IOException {
        Hero hero = new Hero();
        hero.setName("张飞");
        hero.setSkill("英雄比普通人更不正常66666");

        RestClientBuilder rcb = null;
        RestHighLevelClient client = new RestHighLevelClient(rcb);
        rcb = RestClient.builder(new HttpHost("192.168.137.35", 9200, "http"));


        //组装添加文档的请求
        IndexRequest request = new IndexRequest("hero")//hero 是索引名
                .id("4")//指定id
                .source(JSONObject.toJSONString(hero), XContentType.JSON);//数据内容json格式
        //执行添加文档的命令
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        //		  使用UpdateRequest,更新指定字段
        //        Map<String,Object> map = new HashMap<String, Object>();
        //        map.put("skill","英雄比普通人更不正常哈哈哈哈");
        //        UpdateRequest updateRequest = new UpdateRequest("hero","4");
        //        updateRequest.doc(map);
        //        UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);

        //返回结果
        System.out.println(response);
        client.close();
    }

    private static void createDoc() throws IOException {
        Hero hero = new Hero();
        hero.setName("庞德");
        hero.setSkill("关羽有刀，我也有刀");

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.137.35", 9200, "http")));

        //组装添加文档的请求
        IndexRequest request = new IndexRequest("hero")//hero 是索引名
                .id("2")//指定id
                .source(JSONObject.toJSONString(hero), XContentType.JSON);//数据内容json格式
        //执行添加文档的命令
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        //返回结果
        System.out.println(response);
        client.close();

    }
}
