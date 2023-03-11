package com.qxy;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: SayHello
 * @Date: 2023/3/11 10:20
 * @Introduction: java api操作es
 */
@SpringBootTest(classes = ElasticsearchApplication.class)
@RunWith(SpringRunner.class)
@Slf4j
public class DocumentManageTest {
    @Autowired
    RestHighLevelClient client;

    /**
     * 预先在kibana执行以下语句：
     * PUT /springboot/_doc/1
     * {
     * "name":"jack",
     * "age":18,
     * "hobby":["唱","跳","rap"]
     * }
     */
    @Test
    public void testUse() throws IOException {
        //1、创建请求
        GetRequest getRequest = new GetRequest("springboot", "1");
        //2、执行请求
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        //3、获取结果
        if (getResponse.isExists()) {
            log.info("springboot索引的id为1的记录为: {}", getResponse.getSource());
        }
    }

    /**
     * 测试GET请求
     *
     * @throws IOException IOException
     */
    @Test
    public void testGet() throws IOException {
        //1、创建请求
        GetRequest getRequest = new GetRequest("springboot", "1");
        //2、执行请求
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        //3、获取结果
        if (response.isExists()) {
            log.info("index:{}", response.getIndex());
            log.info("id:{}", response.getId());
            log.info("type:{}", response.getType());
            log.info("version:{}", response.getVersion());
            log.info("source:{}", response.getSource());
            log.info("以string获取-sourceString:{}", response.getSourceAsString());
            log.info("以bytes[]获取-sourceBytes:{}", response.getSourceAsBytes());
            log.info("以map获取-sourceMap:{}", response.getSourceAsMap());
        }
    }

    /**
     * 测试返回可选参数
     *
     * @throws IOException IOException
     */
    @Test
    public void testGetSource() throws IOException {
        //1、创建请求
        GetRequest getRequest = new GetRequest("springboot", "1");
        //定制可选参数
        String[] includes = new String[]{"name"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fsc = new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fsc);
        //2、执行请求
        GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
        if (response.isExists()) {
            //3、获取结果
            log.info("source: {}", response.getSource());
        }
    }

    /**
     * 异步执行
     *
     * @throws IOException IOException
     */
    @Test
    public void testGetAsync() throws IOException {
        //1、创建请求
        GetRequest getRequest = new GetRequest("springboot", "1");
        //2、执行请求
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse documentFields) {
                //成功时的响应
                log.info("source: {}", documentFields.getSource());
            }

            @Override
            public void onFailure(Exception e) {
                //失败时的异常
                e.printStackTrace();
            }
        };
        client.getAsync(getRequest, RequestOptions.DEFAULT, listener);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试添加数据：构建请求体的方法共有四种，推荐使用map
     *
     * @throws IOException IOException
     */
    @Test
    public void testAdd() throws IOException {
        //1、创建请求
        IndexRequest request = new IndexRequest("springboot");
        //request.id("2");
        request.id("5");
        //2、创建请求体
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "rose");
        jsonMap.put("age", 21);
        jsonMap.put("hobby", new String[]{"吃饭", "睡觉", "打豆豆"});
        //塞入数据
        request.source(jsonMap, XContentType.JSON);
        //可选参数
        //request.timeout(TimeValue.timeValueSeconds(1));
        //request.version(2);
        //request.versionType(VersionType.EXTERNAL);
        //3、执行请求
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        //判断执行类型
        if (response.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("创建成功");
            log.info("Id: {}", response.getId());
            log.info("Index: {}", response.getIndex());
            log.info("Version(): {}", response.getVersion());
            log.info("Result: {}", response.getResult());
        } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("修改成功");
            log.info("Id: {}", response.getId());
            log.info("Index: {}", response.getIndex());
            log.info("Version(): {}", response.getVersion());
            log.info("Result: {}", response.getResult());
        } else {
            log.info("其它操作");
        }
        //判断分片是否也执行
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            log.info("处理成功的分片数 < 总分片数....请注意!!!!");
        }
        //判断失败
        if (shardInfo.getFailed() > 0) {
            log.info("分片处理有失败的操作");
            ReplicationResponse.ShardInfo.Failure[] failures = shardInfo.getFailures();
            for (ReplicationResponse.ShardInfo.Failure failure : failures) {
                log.info("reason: {}", failure.reason());
            }
        }
    }

    /**
     * 测试异步添加数据：构建请求体的方法共有四种，推荐使用map
     *
     * @throws IOException IOException
     */
    @Test
    public void testAddAsync() throws IOException {
        //1、创建请求
        IndexRequest request = new IndexRequest("springboot");
        request.id("3");
        //2、创建请求体
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "shawn");
        jsonMap.put("age", 22);
        jsonMap.put("hobby", new String[]{"1", "2", "3"});
        //塞入数据
        request.source(jsonMap, XContentType.JSON);
        //可选参数
        request.timeout(TimeValue.timeValueSeconds(1));
        request.version(2);
        request.versionType(VersionType.EXTERNAL);
        //3、执行请求
        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                log.info("Id: {}", indexResponse.getId());
                log.info("Index: {}", indexResponse.getIndex());
                log.info("Version(): {}", indexResponse.getVersion());
                log.info("Result: {}", indexResponse.getResult());
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        client.indexAsync(request, RequestOptions.DEFAULT, listener);
        try {
            //不休眠就会立即关闭,回调函数就不会执行了
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试更新
     *
     * @throws IOException IOException
     */
    @Test
    public void testUpdate() throws IOException {
        //1、创建请求
        UpdateRequest request = new UpdateRequest("springboot", "5");
        //2、创建请求体
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "roseroserose罗捷女士");
        //塞入数据
        request.doc(jsonMap);

        //可选参数
        request.timeout(TimeValue.timeValueSeconds(1));
        request.retryOnConflict(3);
        //3、执行请求
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        //判断执行类型
        if (response.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("创建成功");
            log.info("Id: {}", response.getId());
            log.info("Index: {}", response.getIndex());
            log.info("Version(): {}", response.getVersion());
            log.info("Result: {}", response.getResult());
        } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("修改成功");
            log.info("Id: {}", response.getId());
            log.info("Index: {}", response.getIndex());
            log.info("Version(): {}", response.getVersion());
            log.info("Result: {}", response.getResult());
        } else if (response.getResult() == DocWriteResponse.Result.DELETED) {
            log.info("删除成功");
            log.info("Id: {}", response.getId());
            log.info("Index: {}", response.getIndex());
            log.info("Version(): {}", response.getVersion());
            log.info("Result: {}", response.getResult());
        } else {
            log.info("NOT FOUND");
        }
    }

    /**
     * 测试删除
     *
     * @throws IOException IOException
     */
    @Test
    public void testDelete() throws IOException {
        //1、创建请求
        DeleteRequest request = new DeleteRequest("springboot", "5");

        //可选参数
        //3、执行请求
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        //判断执行类型
        if (response.getResult() == DocWriteResponse.Result.CREATED) {
            log.info("创建成功");
            log.info("Id: {}", response.getId());
            log.info("Index: {}", response.getIndex());
            log.info("Version(): {}", response.getVersion());
            log.info("Result: {}", response.getResult());
        } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            log.info("修改成功");
            log.info("Id: {}", response.getId());
            log.info("Index: {}", response.getIndex());
            log.info("Version(): {}", response.getVersion());
            log.info("Result: {}", response.getResult());
        } else if (response.getResult() == DocWriteResponse.Result.DELETED) {
            log.info("删除成功");
            log.info("Id: {}", response.getId());
            log.info("Index: {}", response.getIndex());
            log.info("Version(): {}", response.getVersion());
            log.info("Result: {}", response.getResult());
        } else {
            log.info("NOT FOUND");
        }
    }

    /**
     * 测试批量增加
     *
     * @throws IOException IOException
     */
    @Test
    public void testBulkADD() throws IOException {
        //1、创建请求
        BulkRequest bulkRequest = new BulkRequest();
        Map<String, Object> map = new HashMap<>();
        map.put("name", "101010");
        map.put("age", 18);
        map.put("hobby", new String[]{"1", "2", "3"});

        Map<String, Object> map2 = new HashMap<>();
        map2.put("name", "111111");
        map2.put("age", 18);
        map2.put("hobby", new String[]{"1", "2", "3"});
        Map<String, Object> map3 = new HashMap<>();
        map3.put("name", "121212");
        map3.put("age", 18);
        map3.put("hobby", new String[]{"1", "2", "3"});
        //2、添加数据
        bulkRequest.add(new IndexRequest("springboot").id("10").source(map, XContentType.JSON));
        bulkRequest.add(new IndexRequest("springboot").id("11").source(map2, XContentType.JSON));
        bulkRequest.add(new IndexRequest("springboot").id("12").source(map3, XContentType.JSON));

        //3、执行请求
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        for (BulkItemResponse itemResponse : bulkResponse) {
            DocWriteResponse response = itemResponse.getResponse();
            //判断执行类型
            switch (response.getResult()) {
                case CREATED:
                    log.info("创建成功-INDEX:{}", response.getIndex());
                    break;
                case UPDATED:
                    log.info("修改成功-UPDATE:{}", response.getIndex());
                    break;
                case DELETED:
                    log.info("删除成功-DELETE:{}", response.getIndex());
                    break;
                case NOT_FOUND:
                    log.info("NOT FOUND");
                    break;
                default:
                    log.info("ERROR OPERATE");
                    break;
            }
        }
    }

    /**
     * 测试批量查询
     *
     * @throws IOException IOException
     */
    @Test
    public void testMGet() throws IOException {
        //1、创建请求
        MultiGetRequest request = new MultiGetRequest();
        request.add(new MultiGetRequest.Item("springboot", "_doc", "10"));
        request.add(new MultiGetRequest.Item("springboot", "_doc", "11"));
        request.add(new MultiGetRequest.Item("springboot", "_doc", "12"));

        //2、执行请求
        MultiGetResponse responses = client.mget(request, RequestOptions.DEFAULT);
        for (MultiGetItemResponse response : responses) {
            GetResponse getResponse = response.getResponse();
            if (getResponse.isExists()) {
                log.info("source: {}", getResponse.getSource());
            }
        }
    }

    /**
     * 测试批量修改
     *
     * @throws IOException IOException
     */
    @Test
    public void testBulkUpdate() throws IOException {
        //1、创建请求
        BulkRequest bulkRequest = new BulkRequest();
        //2、修改数据
        bulkRequest.add(new UpdateRequest("springboot", "10").doc(XContentType.JSON, "name", "10"));
        bulkRequest.add(new UpdateRequest("springboot", "11").doc(XContentType.JSON, "name", "11"));
        bulkRequest.add(new UpdateRequest("springboot", "12").doc(XContentType.JSON, "name", "12"));

        //3、执行请求
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        for (BulkItemResponse itemResponse : bulkResponse) {
            DocWriteResponse response = itemResponse.getResponse();
            //判断执行类型
            switch (response.getResult()) {
                case CREATED:
                    log.info("创建成功-INDEX:{}", response.getIndex());
                    break;
                case UPDATED:
                    log.info("修改成功-UPDATE:{}", response.getIndex());
                    break;
                case DELETED:
                    log.info("删除成功-DELETE:{}", response.getIndex());
                    break;
                case NOT_FOUND:
                    log.info("NOT FOUND");
                    break;
                default:
                    log.info("ERROR OPERATE");
                    break;
            }
        }
    }

    /**
     * 测试批量删除
     *
     * @throws IOException IOException
     */
    @Test
    public void testBulkDelete() throws IOException {
        //1、创建请求
        BulkRequest bulkRequest = new BulkRequest();
        //2、修改数据
        bulkRequest.add(new DeleteRequest("springboot", "10"));
        bulkRequest.add(new DeleteRequest("springboot", "11"));
        bulkRequest.add(new DeleteRequest("springboot", "12"));

        //3、执行请求
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        for (BulkItemResponse itemResponse : bulkResponse) {
            DocWriteResponse response = itemResponse.getResponse();
            //判断执行类型
            switch (response.getResult()) {
                case CREATED:
                    log.info("创建成功-INDEX:{}", response.getIndex());
                    break;
                case UPDATED:
                    log.info("修改成功-UPDATE:{}", response.getIndex());
                    break;
                case DELETED:
                    log.info("删除成功-DELETE:{}", response.getIndex());
                    break;
                case NOT_FOUND:
                    log.info("NOT FOUND");
                    break;
                default:
                    log.info("ERROR OPERATE");
                    break;
            }
        }
    }
}
