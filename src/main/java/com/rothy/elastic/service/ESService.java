package com.rothy.elastic.service;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;


@Service
public class ESService {
    @Autowired
    private RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http")));

    private static final String MY_INDEX = "my_index";
    private static final String DOC_TYPE = "_doc";



    public String createIndexWithNori(String indexName) throws IOException {

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName)
                .settings(Settings.builder()
                        .put("index.analysis.analyzer.default.type", "nori")
                        .put("index.analysis.analyzer.default.decompound_mode", "mixed")
                        .put("index.analysis.analyzer.default.stopwords", "_korean_")
                        .build());


        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("message")
                .field("type", "text")
                .field("analyzer", "nori")
                .endObject()
                .endObject()
                .endObject();
        createIndexRequest.mapping(mappingBuilder);

        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        return createIndexResponse.toString();
    }


    public String getSearchResult(String field, String q) throws IOException {

        SearchRequest searchRequest = new SearchRequest(MY_INDEX)
                .source( new SearchSourceBuilder()
                                .query(QueryBuilders.matchQuery(field, q))
                                .sort(DOC_TYPE, SortOrder.ASC));

        // 인덱스 확인
        GetIndexRequest getIndexRequest = new GetIndexRequest(MY_INDEX);
        boolean indexExists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (!indexExists) {
            return "인덱스 없음~";
        }

        // 검색 요청 실행
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        return response.toString();
    }


    public String insertDocument(String indexName, Long id, Map<String, Object> source) throws IOException {

        IndexRequest indexRequest = new IndexRequest(MY_INDEX)
                .id(String.valueOf(id))
                .source(source)
                .timeout(TimeValue.timeValueSeconds(30));
        // 도큐먼트 추가 요청 실행
        IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

        return response.toString();
    }

    public String insertBulk (String indexName, List<Map<String, Object>> sources) throws IOException {

        BulkRequest bulkReq = new BulkRequest();

        for (Map<String, Object> source : sources) {
            IndexRequest indexReq = new IndexRequest(indexName)
                    .source(source, XContentType.JSON);
            bulkReq.add(indexReq);
        }

        BulkResponse response = restHighLevelClient.bulk(bulkReq, RequestOptions.DEFAULT);

        return response.toString();
    }

}
