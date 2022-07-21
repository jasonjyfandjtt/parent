package com.javasm;


import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

public class Instance {
    public static void main(String[] args) throws Exception {
      //  startIndex();
     //   getIndex();
      //  deleteIndex();
        existIndex();
    }

    private static void existIndex() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.137.35",9200,"http")));
        GetIndexRequest request = new GetIndexRequest("wangzhe");//fengxiansheng 是索引名
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        // true：存在，false：不存在
        System.out.println(exists);
        client.close();
    }

    private static void deleteIndex() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.137.35",9200,"http")));
        DeleteIndexRequest request = new DeleteIndexRequest("wangzhe");//wangzhe 是索引名
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(response.isAcknowledged());
        client.close();
    }

    private static void getIndex() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.137.35",9200,"http")));
        GetIndexRequest request = new GetIndexRequest("wangzhe");//wangzhe 是索引名
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
        //获取结果
        Map<String, MappingMetadata> mappings = response.getMappings();
        for(String key : mappings.keySet()){
            System.out.println(key + "----" + mappings.get(key).sourceAsMap());
        }
        client.close();
    }

    private static void startIndex() throws IOException {
        //1. 创建ES连接客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.168.137.35",9200,"http")));
        //2. 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("caocao");//是索引名
        //2.1 mapping
        String mapping = "{\n" +
                "  \"properties\":{\n" +
                "    \"name\":{\n" +
                "      \"type\":\"keyword\"\n" +
                "    },\n" +
                "    \"skill\":{\n" +
                "      \"type\":\"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        //设置 mapping 的格式是json
        request.mapping(mapping, XContentType.JSON);
        //2.2 使用client创建索引
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        //3. 返回true,表示创建成功
        System.out.println(response.isAcknowledged());
        //4. 关闭客户端
        client.close();
    }

}
