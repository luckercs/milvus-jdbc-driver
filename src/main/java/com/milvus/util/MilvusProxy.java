package com.milvus.util;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DescribeDatabaseReq;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.ListPartitionsReq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// demo: https://github.com/milvus-io/milvus-sdk-java/tree/master/examples/src/main/java/io/milvus/v2


public class MilvusProxy {
    private final String uri;
    private final String user;
    private final String password;
    private final String db;
    private final int timeoutMs;
    private final boolean useSSL;


    public MilvusProxy(String uri, String user, String password, String db, int timeoutMs, boolean useSSL) {
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.db = db;
        this.timeoutMs = timeoutMs;
        this.useSSL = useSSL;
    }

    public String getDb() {
        return db;
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
    public void closeClient(MilvusClientV2 milvusClient){
        try {
            milvusClient.close();
        }catch (Exception e){
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














    public List<String> getAllDbs() {
        MilvusClientV2 milvusClient = getMilvusClient("default");
        List<String> databaseNames = milvusClient.listDatabases().getDatabaseNames();
        milvusClient.close();
        return databaseNames;
    }



    public boolean hasCollection(String dbName, String collectionName) {
        MilvusClientV2 milvusClient = getMilvusClient(dbName);
        Boolean res = milvusClient.hasCollection(HasCollectionReq.builder().collectionName(collectionName).build());
        milvusClient.close();
        return res;
    }

    public Map<String, String> getDataBaseSchema(String dbName) {
        MilvusClientV2 milvusClient = getMilvusClient(dbName);
        DescribeDatabaseResp describeDatabaseResp = milvusClient.describeDatabase(DescribeDatabaseReq.builder().databaseName(dbName).build());
        Map<String, String> properties = describeDatabaseResp.getProperties();
        milvusClient.close();
        return properties;
    }

    public void createDatabase(String dbName, Map<String, String> properties) {
        MilvusClientV2 milvusClient = getMilvusClient("default");
        milvusClient.createDatabase(CreateDatabaseReq.builder().databaseName(dbName).properties(properties).build());
        milvusClient.close();
    }


    public void createCollectionFromExistsCollection(MilvusClientV2 milvusClientSrc, MilvusClientV2 milvusClientTarget, String dbName, String collectionName) {
        DescribeCollectionResp describeCollectionResp = milvusClientSrc.describeCollection(DescribeCollectionReq.builder().databaseName(dbName).collectionName(collectionName).build());

        milvusClientTarget.createCollection(CreateCollectionReq.builder().databaseName(dbName).collectionName(collectionName)
                .collectionSchema(describeCollectionResp.getCollectionSchema())
                .autoID(describeCollectionResp.getAutoID())
                .consistencyLevel(describeCollectionResp.getConsistencyLevel())
                .description(describeCollectionResp.getDescription())
                .enableDynamicField(describeCollectionResp.getEnableDynamicField())
                .numShards(describeCollectionResp.getShardsNum())
                .properties(describeCollectionResp.getProperties())
                .build());

        for (String partitionName : milvusClientSrc.listPartitions(ListPartitionsReq.builder().collectionName(collectionName).build())) {
            if (!partitionName.equals("_default")) {
                milvusClientTarget.createPartition(CreatePartitionReq.builder().collectionName(collectionName).partitionName(partitionName).build());
            }
        }

        List<String> Indexes = milvusClientSrc.listIndexes(ListIndexesReq.builder().collectionName(collectionName).build());
        if (Indexes != null && !Indexes.isEmpty()) {
            List<IndexParam> indexParams = new ArrayList<>();
            for (String indexName : Indexes) {
                DescribeIndexResp describeIndexResp = milvusClientSrc.describeIndex(DescribeIndexReq.builder().databaseName(dbName).collectionName(collectionName).indexName(indexName).build());
                DescribeIndexResp.IndexDesc indexSrc = describeIndexResp.getIndexDescByIndexName(indexName);
                IndexParam indexParam = IndexParam.builder().fieldName(indexSrc.getFieldName()).indexName(indexName).indexType(indexSrc.getIndexType()).extraParams(Collections.unmodifiableMap(indexSrc.getExtraParams())).metricType(indexSrc.getMetricType()).build();
                indexParams.add(indexParam);
            }
            milvusClientTarget.createIndex(CreateIndexReq.builder().databaseName(dbName).collectionName(collectionName).indexParams(indexParams).build());
        }
    }

    public static void main(String[] args) {
        MilvusProxy milvusUtil = new MilvusProxy("http://localhost:19530", "");
        milvusUtil.createDatabase("test", null);

//        MilvusClientV2 milvusClient = milvusUtil.getMilvusClient("com.test");
//        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder().build();
//        collectionSchema.addField(AddFieldReq.builder().fieldName("my_id").dataType(DataType.Int64).isPrimaryKey(true).build());
//        collectionSchema.addField(AddFieldReq.builder().fieldName("my_vector").dataType(DataType.FloatVector).dimension(5).build());
//        collectionSchema.addField(AddFieldReq.builder().fieldName("my_varchar").dataType(DataType.VarChar).maxLength(512).build());
//        List<IndexParam> indexParams = new ArrayList<>();
//        indexParams.add(IndexParam.builder().fieldName("my_vector").indexType(IndexParam.IndexType.AUTOINDEX).metricType(IndexParam.MetricType.COSINE).build());
//        milvusClient.createCollection(CreateCollectionReq.builder().databaseName("com.test").collectionName("test2").collectionSchema(collectionSchema).indexParams(indexParams).build());
//        milvusClient.close();
//
//        milvusClient = milvusUtil.getMilvusClient("default");
//        collectionSchema = CreateCollectionReq.CollectionSchema.builder().build();
//        collectionSchema.addField(AddFieldReq.builder().fieldName("my_id").dataType(DataType.Int64).isPrimaryKey(true).build());
//        collectionSchema.addField(AddFieldReq.builder().fieldName("my_vector").dataType(DataType.FloatVector).dimension(5).build());
//        collectionSchema.addField(AddFieldReq.builder().fieldName("my_varchar").dataType(DataType.VarChar).maxLength(512).build());
//        indexParams = new ArrayList<>();
//        indexParams.add(IndexParam.builder().fieldName("my_vector").indexType(IndexParam.IndexType.AUTOINDEX).metricType(IndexParam.MetricType.COSINE).build());
//        milvusClient.createCollection(CreateCollectionReq.builder().databaseName("default").collectionName("hello1").collectionSchema(collectionSchema).indexParams(indexParams).build());
//        milvusClient.createCollection(CreateCollectionReq.builder().databaseName("default").collectionName("hello2").collectionSchema(collectionSchema).indexParams(indexParams).build());
//        milvusClient.close();

//
        List<String> allDbs = milvusUtil.getAllDbs();
        System.out.println(allDbs);
        System.out.println("=================");

//        List<String> allCollections = milvusUtil.findAllCollections("com.test");
//        System.out.println(allCollections);
    }
}
