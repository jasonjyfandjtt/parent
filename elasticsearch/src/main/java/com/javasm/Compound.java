package com.javasm;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Compound {
    public static void main(String[] args) throws Exception {
        bulk();
        importData();
        queryAll();
        termQuery();
        matchQuery();
        likeCard();
        importData2();
        prefixQuery();
        rangeQuery();
        descRangeQuery();
        queryString();
        booleanQuery();
        aggregationQuery();
        highLight();


    }

    private static void highLight() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));

        SearchRequest request = new SearchRequest("hero");// hero：索引名
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("skill", "王者荣耀"));

        //设置高亮条件
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("skill").preTags("<font color='red'>").postTags("</font>");
        builder.highlighter(highlightBuilder);
        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println("一共有：" + hits.getTotalHits().value + " 条数据");

        SearchHit[] arr = hits.getHits();
        for (SearchHit hit : arr) {
            //获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //拿到高亮的字段
            HighlightField addressField = highlightFields.get("skill");
            //获取高亮的数据,是一个数组，我们文档比较简单，取数组的第一个就行了
            String address = addressField.getFragments()[0].string();
            Map<String, Object> map = hit.getSourceAsMap();
            //用高亮的数据替换查询到的结果
            map.put("skill", address);
            System.out.println(map);
        }
        client.close();
    }

    private static void aggregationQuery() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));

        SearchRequest request = new SearchRequest("hero");// hero：索引名
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());

        AggregationBuilder aggBuilder = AggregationBuilders.terms("type_group").field("type").size(10);
        builder.aggregation(aggBuilder);

        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 获取聚合结果
        Aggregation groupResult = response.getAggregations().asMap().get("type_group");
        // 获取分组结果，需要把 groupResult 转换为 Term
        List<? extends Terms.Bucket> buckets = ((Terms) groupResult).getBuckets();
        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.getKey() + "----" + bucket.getDocCount());
        }
        client.close();
    }

    private static void booleanQuery() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));

        SearchRequest request = new SearchRequest("hero");// person：索引名
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // bool 查询，使用 boolQuery 方法
        builder.query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("skill", "王者")).filter(QueryBuilders.termQuery("name", "妲己")));

        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println("一共有：" + hits.getTotalHits().value + " 条数据");
        SearchHit[] arr = hits.getHits();
        for (SearchHit hit : arr) {
            String content = hit.getSourceAsString();//以字符串形式获取数据内容
            System.out.println(content);
        }
        client.close();

    }

    private static void queryString() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));

        SearchRequest request = new SearchRequest("hero");// hero：索引名
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // queryString 查询，使用 queryStringQuery 方法
        //        builder.query(QueryBuilders.queryStringQuery("亚瑟的荣耀")
        //                .field("skill").field("name").analyzer("ik_max_word"));

        // simpleQueryString 查询，使用 simpleQueryStringQuery 方法
        builder.query(QueryBuilders.simpleQueryStringQuery("亚瑟 AND 荣耀").field("skill").field("name").analyzer("ik_max_word"));

        request.source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println("一共有：" + hits.getTotalHits().value + " 条数据");
        SearchHit[] arr = hits.getHits();
        for (SearchHit hit : arr) {
            String content = hit.getSourceAsString();//以字符串形式获取数据内容
            System.out.println(content);
        }
        client.close();
    }

    private static void descRangeQuery() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));

        SearchRequest request = new SearchRequest("hero");// hero：索引名
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // 范围查询，使用 rangeQuery 方法
        builder.query(QueryBuilders.rangeQuery("skill_num").gte(6).lte(7));
        builder.from(0);
        builder.size(5);
        // 排序
        builder.sort("skill_num", SortOrder.DESC);
        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println("一共有：" + hits.getTotalHits().value + " 条数据");
        SearchHit[] arr = hits.getHits();
        for (SearchHit hit : arr) {
            String content = hit.getSourceAsString();//以字符串形式获取数据内容
            System.out.println(content);
        }
        client.close();
    }

    private static void rangeQuery() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));

        SearchRequest request = new SearchRequest("hero");// hero：索引名
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // 范围查询，使用 rangeQuery 方法
        builder.query(QueryBuilders.rangeQuery("skill_num").gte(6).lte(7));
        builder.from(0);
        builder.size(5);
        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println("一共有：" + hits.getTotalHits().value + " 条数据");
        SearchHit[] arr = hits.getHits();
        for (SearchHit hit : arr) {
            String content = hit.getSourceAsString();//以字符串形式获取数据内容
            System.out.println(content);
        }
        client.close();
    }

    private static void importData2() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 6; i < 100; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("name", "嫦娥_" + i);
            map.put("skill", "像做梦一样");
            map.put("skill_num", i);//增加一个字段
            IndexRequest createDoc = new IndexRequest("hero")//hero 是索引名
                    .id(i + "")//指定id
                    .source(map);//参数也可以螫map
            bulkRequest.add(createDoc);
        }
        BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(responses.status());
        client.close();
    }


    private static void prefixQuery() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));

        SearchRequest request = new SearchRequest("hero");// hero：索引名
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // wildcard 查询，使用 wildcardQuery 方法
        //        builder.query(QueryBuilders.wildcardQuery("hero","王?"));
        // 正则查询，使用 regexpQuery 方法
        //        builder.query(QueryBuilders.regexpQuery("hero",".*荣耀"));
        // 前缀查询，使用 prefixQuery 方法
        builder.query(QueryBuilders.prefixQuery("name", "嫦娥_"));
        builder.from(0);
        builder.size(5);
        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println("一共有：" + hits.getTotalHits().value + " 条数据");
        SearchHit[] arr = hits.getHits();
        for (SearchHit hit : arr) {
            String content = hit.getSourceAsString();//以字符串形式获取数据内容
            System.out.println(content);
        }
        client.close();

    }

    private static void likeCard() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));

        SearchRequest request = new SearchRequest("hero");// hero：索引名
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // match 查询，使用 matchQuery 方法
        builder.query(QueryBuilders.wildcardQuery("skill", "王?"));
        builder.from(0);
        builder.size(5);
        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println("一共有：" + hits.getTotalHits().value + " 条数据");
        SearchHit[] arr = hits.getHits();
        for (SearchHit hit : arr) {
            String content = hit.getSourceAsString();//以字符串形式获取数据内容
            System.out.println(content);
        }
        client.close();

    }

    private static void matchQuery() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));

        SearchRequest request = new SearchRequest("hero");// hero：索引名
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // match 查询，使用 matchQuery 方法
        builder.query(QueryBuilders.matchQuery("skill", "王者不可阻挡").operator(Operator.AND));
        builder.from(0);
        builder.size(5);
        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println("一共有：" + hits.getTotalHits().value + " 条数据");
        SearchHit[] arr = hits.getHits();
        for (SearchHit hit : arr) {
            String content = hit.getSourceAsString();//以字符串形式获取数据内容
            System.out.println(content);
        }
        client.close();
    }

    private static void termQuery() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));

        SearchRequest request = new SearchRequest("hero");// hero：索引名
        SearchSourceBuilder builder = new SearchSourceBuilder();
        // term查询，使用 termQuery 方法
        builder.query(QueryBuilders.termQuery("name", "庞德"));
        builder.from(0);
        builder.size(5);
        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        System.out.println("一共有：" + hits.getTotalHits().value + " 条数据");
        SearchHit[] arr = hits.getHits();
        for (SearchHit hit : arr) {
            String content = hit.getSourceAsString();//以字符串形式获取数据内容
            System.out.println(content);
        }
        client.close();
    }

    private static void queryAll() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));

        //1. 创建请求对象
        SearchRequest request = new SearchRequest("hero");// hero：索引名
        //1.1 查询条件构造器
        SearchSourceBuilder builder = new SearchSourceBuilder();
        //1.2 组装查询条件，目前是查询所有
        builder.query(QueryBuilders.matchAllQuery());
        //1.3 设置分页条件
        builder.from(0);
        builder.size(5);
        //1.4 把查询条件放到requst中
        request.source(builder);

        //2. 发送查询请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //3. 拿到返回结果
        SearchHits hits = response.getHits();
        //3.1 输出总条数
        System.out.println("一共有：" + hits.getTotalHits().value + " 条数据");
        //3.2 取出数据内容
        SearchHit[] arr = hits.getHits();
        for (SearchHit hit : arr) {
            String content = hit.getSourceAsString();//以字符串形式获取数据内容
            System.out.println(content);
        }
        client.close();
    }

    private static void importData() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 6; i < 100; i++) {
            Map<String, Object> map = new HashMap<>();
            map.put("name", "嫦娥_" + i);
            map.put("skill", "像做梦一样");
            IndexRequest createDoc = new IndexRequest("hero")//hero 是索引名
                    .id(i + "")//指定id
                    .source(map);//参数也可以螫map
            bulkRequest.add(createDoc);
        }
        BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(responses.status());

        client.close();
    }

    private static void bulk() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.137.35", 9200, "http")));
        //1. 组装bulk请求对象
        BulkRequest bulkRequest = new BulkRequest();
        //1.1 删除id=4的文档
        DeleteRequest delReq = new DeleteRequest("hero", "4");
        bulkRequest.add(delReq);

        //1.2 创建id=5的文档
        Map<String, Object> map = new HashMap<>();
        map.put("name", "甄姬");
        map.put("skill", "小女子尚在阵前，大丈夫却要回城?");
        IndexRequest createDoc = new IndexRequest("hero")//hero 是索引名
                .id("5")//指定id
                .source(map);//参数也可以螫map
        bulkRequest.add(createDoc);

        //1.3 更新id=2的文档
        Map<String, Object> map2 = new HashMap<>();
        map2.put("skill", "没有心,就不会受伤");
        //更新数据也可以使用UpdateRequest,更新指定字段
        UpdateRequest updateRequest = new UpdateRequest("hero", "2");
        updateRequest.doc(map2);
        bulkRequest.add(updateRequest);

        BulkResponse responses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        //输出每个请求的执行结果
        for (BulkItemResponse item : responses.getItems()) {
            System.out.println(item.status());
        }
        client.close();
    }

}
