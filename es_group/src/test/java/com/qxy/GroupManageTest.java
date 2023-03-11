package com.qxy;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.*;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * @Author: SayHello
 * @Date: 2023/3/11 13:52
 * @Introduction: java api操作分组聚合 [预先插入数据在data.txt]
 */
@SpringBootTest(classes = ElasticsearchApplication.class)
@RunWith(SpringRunner.class)
@Slf4j
public class GroupManageTest {
    @Autowired
    RestHighLevelClient client;

    /**
     * 需求一：按照颜色分组，计算每个颜色卖出的个数
     */
    @Test
    public void testQuestion1() throws IOException {
        //1、构建请求
        SearchRequest request = new SearchRequest("tvs");
        //请求体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupby_color").field("color");
        searchSourceBuilder.aggregation(termsAggregationBuilder);

        request.source(searchSourceBuilder);
        //2、执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //3、返回结果
        Aggregations aggregations = response.getAggregations();
        Terms groupby_color = aggregations.get("groupby_color");
        List<? extends Terms.Bucket> buckets = groupby_color.getBuckets();
        System.out.println("-----------------");
        for (Terms.Bucket bucket : buckets) {
            String key = (String) bucket.getKey();
            log.info("key: {}", key);
            long docCount = bucket.getDocCount();
            log.info("docCount: {}", docCount);
            System.out.println("-----------------");
        }
    }

    /**
     * 需求二：按照颜色分组，计算每个颜色卖出的个数,每个颜色卖出的平均价格
     */
    @Test
    public void testQuestion2() throws IOException {
        //1、构建请求
        SearchRequest request = new SearchRequest("tvs");
        //请求体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupby_color").field("color");
        AggregationBuilder subAggregationBuilder = AggregationBuilders.avg("avg_price").field("price");
        termsAggregationBuilder.subAggregation(subAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);

        request.source(searchSourceBuilder);
        //2、执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //3、返回结果
        Aggregations aggregations = response.getAggregations();
        Terms groupby_color = aggregations.get("groupby_color");
        List<? extends Terms.Bucket> buckets = groupby_color.getBuckets();
        System.out.println("-----------------");
        for (Terms.Bucket bucket : buckets) {
            String key = (String) bucket.getKey();
            log.info("key: {}", key);

            long docCount = bucket.getDocCount();
            log.info("docCount: {}", docCount);

            Aggregations subAggregations = bucket.getAggregations();
            Avg avg_price = subAggregations.get("avg_price");
            double price = avg_price.getValue();
            log.info("avg_price: {}", price);
            System.out.println("-----------------");
        }
    }

    /**
     * 需求三：按照颜色分组，计算每个颜色卖出的个数,每个颜色卖出的平均价格、最大价格、最小价格、总价格
     */
    @Test
    public void testQuestion3() throws IOException {
        //1、构建请求
        SearchRequest request = new SearchRequest("tvs");
        //请求体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupby_color").field("color");
        AggregationBuilder subAvgAggregationBuilder = AggregationBuilders.avg("avg_price").field("price");
        AggregationBuilder subMaxAggregationBuilder = AggregationBuilders.max("max_price").field("price");
        AggregationBuilder subMinAggregationBuilder = AggregationBuilders.min("min_price").field("price");
        AggregationBuilder subSumAggregationBuilder = AggregationBuilders.sum("sum_price").field("price");
        termsAggregationBuilder.subAggregation(subAvgAggregationBuilder);
        termsAggregationBuilder.subAggregation(subMaxAggregationBuilder);
        termsAggregationBuilder.subAggregation(subMinAggregationBuilder);
        termsAggregationBuilder.subAggregation(subSumAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);

        request.source(searchSourceBuilder);
        //2、执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //3、返回结果
        Aggregations aggregations = response.getAggregations();
        Terms groupby_color = aggregations.get("groupby_color");
        List<? extends Terms.Bucket> buckets = groupby_color.getBuckets();
        System.out.println("-----------------");
        for (Terms.Bucket bucket : buckets) {
            String key = (String) bucket.getKey();
            log.info("key: {}", key);

            long docCount = bucket.getDocCount();
            log.info("docCount: {}", docCount);

            Aggregations subAggregations = bucket.getAggregations();
            Avg avg = subAggregations.get("avg_price");
            double avg_price = avg.getValue();
            log.info("avg_price: {}", avg_price);


            Max max = subAggregations.get("max_price");
            double max_price = max.getValue();
            log.info("max_price: {}", max_price);

            Min min = subAggregations.get("min_price");
            double min_price = min.getValue();
            log.info("min_price: {}", min_price);

            Sum sum = subAggregations.get("sum_price");
            double sum_price = sum.getValue();
            log.info("sum_price: {}", sum_price);
            System.out.println("-----------------");
        }
    }

    /**
     * 需求四：按照售价每2000元划分范围，算出每个区间的销售总额 histogram
     */
    @Test
    public void testQuestion4() throws IOException {
        //1、构建请求
        SearchRequest request = new SearchRequest("tvs");
        //请求体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        HistogramAggregationBuilder histogramAggregationBuilder = AggregationBuilders.histogram("price_range").field("price").interval(2000);
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("sale").field("price");

        histogramAggregationBuilder.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(histogramAggregationBuilder);

        request.source(searchSourceBuilder);
        //2、执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //3、返回结果
        Aggregations aggregations = response.getAggregations();
        Histogram price_ranger = aggregations.get("price_range");
        List<? extends Histogram.Bucket> buckets = price_ranger.getBuckets();
        System.out.println("-----------------");
        for (Histogram.Bucket bucket : buckets) {
            Double key = (Double) bucket.getKey();
            log.info("key: {}", key);

            long docCount = bucket.getDocCount();
            log.info("docCount: {}", docCount);

            Aggregations subAggregations = bucket.getAggregations();
            Sum sale = subAggregations.get("sale");
            double price = sale.getValue();
            log.info("sale: {}", price);
            System.out.println("-----------------");
        }
    }

    /**
     * 需求五：计算每个季度的销售总额 histogram
     */
    @Test
    public void testQuestion5() throws IOException {
        //1、构建请求
        SearchRequest request = new SearchRequest("tvs");
        //请求体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder = AggregationBuilders
                .dateHistogram("sales")
                .field("sold_date")
                .calendarInterval(DateHistogramInterval.QUARTER)
                .format("yyyy-MM-dd")
                .minDocCount(0)
                .extendedBounds(new ExtendedBounds("2019-01-01", "2020-12-31"));
        SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum("income").field("price");
        dateHistogramAggregationBuilder.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(dateHistogramAggregationBuilder);

        request.source(searchSourceBuilder);
        //2、执行
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        //3、返回结果
        Aggregations aggregations = response.getAggregations();
        Histogram price_ranger = aggregations.get("sales");
        List<? extends Histogram.Bucket> buckets = price_ranger.getBuckets();
        System.out.println("-----------------");
        for (Histogram.Bucket bucket : buckets) {
            ZonedDateTime key = (ZonedDateTime) bucket.getKey();
            log.info("key: {}", key);

            String keyAsString = (String) bucket.getKeyAsString();
            log.info("date: {}", keyAsString);

            long docCount = bucket.getDocCount();
            log.info("docCount: {}", docCount);

            Aggregations subAggregations = bucket.getAggregations();
            Sum sale = subAggregations.get("income");
            double price = sale.getValue();
            log.info("income: {}", price);
            System.out.println("-----------------");
        }
    }
}
