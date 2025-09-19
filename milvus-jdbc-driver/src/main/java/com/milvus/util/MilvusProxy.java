package com.milvus.util;

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;

import java.util.List;

// https://github.com/milvus-io/milvus-sdk-java/tree/master/examples/src/main/java/io/milvus/v2

public class MilvusProxy {
    private final String uri;
    private final String user;
    private final String password;
    private final String db;
    private final int timeoutMs;
    private final boolean useSSL;
    private final int batchSize;


    public MilvusProxy(String uri, String user, String password, String db, int timeoutMs, boolean useSSL, int batchSize) {
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.db = db;
        this.timeoutMs = timeoutMs;
        this.useSSL = useSSL;
        this.batchSize = batchSize;
    }

    public String getDb() {
        return db;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public MilvusClientV2 getClient() {
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(uri)
                .username(user)
                .password(password)
                .dbName(db)
                .connectTimeoutMs(timeoutMs)
                .build();
        return new MilvusClientV2(connectConfig);
    }

    public void closeClient(MilvusClientV2 milvusClient) {
        try {
            milvusClient.close();
        } catch (Exception e) {
        }
    }

    public void closeQueryIterator(QueryIterator queryIterator) {
        try {
            queryIterator.close();
        } catch (Exception e) {
        }
    }

    public void closeSearchIterator(SearchIterator searchIterator) {
        try {
            searchIterator.close();
        } catch (Exception e) {
        }
    }

    public List<String> getAllCollections() {
        MilvusClientV2 milvusClient = getClient();
        List<String> collectionNames = milvusClient.listCollections().getCollectionNames();
        closeClient(milvusClient);
        return collectionNames;
    }

    public DescribeCollectionResp getCollectionDesc(String collectionName) {
        MilvusClientV2 milvusClient = getClient();
        DescribeCollectionResp describeCollectionResp = milvusClient.describeCollection(DescribeCollectionReq.builder().databaseName(db).collectionName(collectionName).build());
        closeClient(milvusClient);
        return describeCollectionResp;
    }

    public DescribeIndexResp getIndexDesc(String collectionName, String fieldName) {
        MilvusClientV2 milvusClient = getClient();
        DescribeIndexResp describeIndexResp = milvusClient.describeIndex(DescribeIndexReq.builder().databaseName(db).collectionName(collectionName).fieldName(fieldName).build());
        closeClient(milvusClient);
        return describeIndexResp;
    }
}
