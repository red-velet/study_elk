package com.qxy;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
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
 * @Date: 2023/3/11 13:52
 * @Introduction:
 */
@SpringBootTest(classes = ElasticsearchApplication.class)
@RunWith(SpringRunner.class)
@Slf4j
public class IndexManageTest {
    @Autowired
    RestHighLevelClient client;

    @Test
    public void testCreateIndex() throws IOException {
        //1、创建请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("book_v1");
        createIndexRequest.settings(Settings.builder().
                put("number_of_shards", "1").
                put("number_of_replicas", "1"));
        Map<String, Object> message = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> field1 = new HashMap<>();
        field1.put("type", "text");
        Map<String, Object> field2 = new HashMap<>();
        field2.put("type", "text");
        properties.put("field1", field1);
        properties.put("field2", field2);
        message.put("properties", properties);
        createIndexRequest.mapping(message);
        createIndexRequest.alias(new Alias("book"));
        //添加其它参数
        createIndexRequest.setTimeout(TimeValue.timeValueSeconds(3));
        createIndexRequest.setMasterTimeout(TimeValue.timeValueSeconds(3));
        createIndexRequest.waitForActiveShards(ActiveShardCount.ALL);
        //2、执行操作
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        //3、返回结果
        boolean acknowledged = createIndexResponse.isAcknowledged();
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
        String index = createIndexResponse.index();
        log.info("acknowledged: {}", acknowledged);
        log.info("shardsAcknowledged: {}", shardsAcknowledged);
        log.info("index: {}", index);
    }

    @Test
    public void testCreateIndexAsync() throws IOException, InterruptedException {
        //1、创建请求
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("book_v1");
        createIndexRequest.settings(Settings.builder().
                put("number_of_shards", "1").
                put("number_of_replicas", "1"));
        Map<String, Object> message = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        Map<String, Object> field1 = new HashMap<>();
        field1.put("type", "text");
        Map<String, Object> field2 = new HashMap<>();
        field2.put("type", "text");
        properties.put("field1", field1);
        properties.put("field2", field2);
        message.put("properties", properties);
        createIndexRequest.mapping(message);
        createIndexRequest.alias(new Alias("book"));
        //添加其它参数
        createIndexRequest.setTimeout(TimeValue.timeValueSeconds(3));
        createIndexRequest.setMasterTimeout(TimeValue.timeValueSeconds(3));
        //创建监听器
        IndicesClient indices = client.indices();
        ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                //3、返回结果
                boolean acknowledged = createIndexResponse.isAcknowledged();
                boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
                String index = createIndexResponse.index();
                log.info("acknowledged: {}", acknowledged);
                log.info("shardsAcknowledged: {}", shardsAcknowledged);
                log.info("index: {}", index);
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        //2、执行操作
        indices.createAsync(createIndexRequest, RequestOptions.DEFAULT, listener);
        Thread.sleep(3000);
    }

    @Test
    public void testDeleteIndex() throws IOException {
        //1、创建请求
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("book_v1");
        //2、执行操作
        AcknowledgedResponse response = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        //3、返回结果
        boolean acknowledged = response.isAcknowledged();
        log.info("acknowledged :{}", acknowledged);
    }

    @Test
    public void testDeleteIndexAsync() throws IOException, InterruptedException {
        //1、创建请求
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("book_v1");
        //2、执行操作
        ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                //3、返回结果
                boolean acknowledged = acknowledgedResponse.isAcknowledged();
                log.info("acknowledged :{}", acknowledged);
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
            }
        };
        client.indices().deleteAsync(deleteIndexRequest, RequestOptions.DEFAULT, listener);
        Thread.sleep(2000);
    }

    @Test
    public void test() {
        
    }

    /**
     * Indices Exists API
     *
     * @throws IOException IOException
     */
    @Test
    public void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("book_v1");
        request.local(false);//从主节点返回本地信息或检索状态
        request.humanReadable(true);//以适合人类的格式返回结果
        request.includeDefaults(false);//是否返回每个索引的所有默认设置
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        log.info("exists: {}", exists);
    }

    /**
     * Indices Close API:关闭索引-冻结
     *
     * @throws IOException IOException
     */
    @Test
    public void testCloseIndex() throws IOException {
        CloseIndexRequest request = new CloseIndexRequest("book_v1");
        AcknowledgedResponse closeIndexResponse = client.indices().close(request, RequestOptions.DEFAULT);
        boolean acknowledged = closeIndexResponse.isAcknowledged();
        System.out.println("!!!!!!!!!" + acknowledged);
    }

    /**
     * Indices Open API:开启索引-解冻
     *
     * @throws IOException IOException
     */
    @Test
    public void testOpenIndex() throws IOException {
        OpenIndexRequest request = new OpenIndexRequest("book_v1");
        OpenIndexResponse openIndexResponse = client.indices().open(request, RequestOptions.DEFAULT);
        boolean acknowledged = openIndexResponse.isAcknowledged();
        System.out.println("!!!!!!!!!" + acknowledged);
    }
}
