package com.qxy.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author: SayHello
 * @Date: 2023/3/11 10:25
 * @Introduction: 向容器注入es客户端
 */
@Configuration
public class ElasticsearchConfig {
    @Value("${qxy.elasticsearch.hostList}")
    private String hostList;

    @Bean(destroyMethod = "close")
    public RestHighLevelClient getRestHighLevelClient() {
        //可能有多个host,使用逗号分割
        String[] hostArray = hostList.split(",");
        //一个host对应一个HttpHost
        HttpHost[] httpHosts = new HttpHost[hostArray.length];
        for (int i = 0; i < httpHosts.length; i++) {
            //将hostList的格式拆解: ip:host -> ip host
            String[] split = hostArray[i].split(":");
            String ip = split[0];
            int port = Integer.parseInt(split[1]);
            httpHosts[i] = new HttpHost(ip, port);
        }
        return new RestHighLevelClient(RestClient.builder(httpHosts));
    }
}
