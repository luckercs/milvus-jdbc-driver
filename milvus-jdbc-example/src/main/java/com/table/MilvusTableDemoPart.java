package com.table;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.milvus.util.CommonUtils;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.response.InsertResp;

import java.util.*;

public class MilvusTableDemoPart {
    private static final String COLLECTION_NAME = "milvus_table_2";
    private static final String PARTITION_NAME_1 = "part1";
    private static final String PARTITION_NAME_2 = "part2";
    private static final String PARTITION_NAME_3 = "part3";
    private static final Integer VECTOR_DIM = 128;
    private static final Integer INSERT_NUM = 240;

    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .dbName("default")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f1_bool")
                .dataType(DataType.Bool)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f2_int8")
                .dataType(DataType.Int8)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f3_int16")
                .dataType(DataType.Int16)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f4_int32")
                .dataType(DataType.Int32)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f5_int64")
                .dataType(DataType.Int64)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f6_float")
                .dataType(DataType.Float)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f7_double")
                .dataType(DataType.Double)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f8_varchar")
                .dataType(DataType.VarChar)
                .maxLength(1024)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f9_array_int32")
                .dataType(DataType.Array)
                .elementType(DataType.Int32)
                .maxCapacity(10)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f9_array_varchar")
                .dataType(DataType.Array)
                .elementType(DataType.VarChar)
                .maxCapacity(10)
                .maxLength(100)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f10_json")
                .dataType(DataType.JSON)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f11_floatvector")
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f12_binaryvector")
                .dataType(DataType.BinaryVector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f13_float16vector")
                .dataType(DataType.Float16Vector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f14_bfloat16vector")
                .dataType(DataType.BFloat16Vector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("f15_sparsefloatvector")
                .dataType(DataType.SparseFloatVector)
                .build());

        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 64);
        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName("f11_floatvector")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.COSINE)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName("f12_binaryvector")
                .indexType(IndexParam.IndexType.BIN_IVF_FLAT)
                .metricType(IndexParam.MetricType.HAMMING)
                .extraParams(extraParams)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName("f13_float16vector")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extraParams)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName("f14_bfloat16vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName("f15_sparsefloatvector")
                .indexType(IndexParam.IndexType.SPARSE_WAND)
                .metricType(IndexParam.MetricType.IP)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");


        client.createPartition(CreatePartitionReq.builder().collectionName(COLLECTION_NAME).partitionName(PARTITION_NAME_1).build());
        client.createPartition(CreatePartitionReq.builder().collectionName(COLLECTION_NAME).partitionName(PARTITION_NAME_2).build());
        client.createPartition(CreatePartitionReq.builder().collectionName(COLLECTION_NAME).partitionName(PARTITION_NAME_3).build());


        List<JsonObject> rows = genRows(INSERT_NUM);
        InsertResp resp = client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionName(PARTITION_NAME_1)
                .data(rows.subList(0, rows.size()/3))
                .build());
        List<Object> ids = resp.getPrimaryKeys();
        System.out.println("Collection insertRows part1 done, insertCount:" + ids.size());

        resp = client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionName(PARTITION_NAME_2)
                .data(rows.subList(rows.size()/3, rows.size()/3*2))
                .build());
        ids = resp.getPrimaryKeys();
        System.out.println("Collection insertRows part2 done, insertCount:" + ids.size());

        resp = client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionName(PARTITION_NAME_3)
                .data(rows.subList(rows.size()/3*2, rows.size()))
                .build());
        ids = resp.getPrimaryKeys();
        System.out.println("Collection insertRows part3 done, insertCount:" + ids.size());

        System.out.println("ALL Finished");
    }

    private static List<JsonObject> genRows(int count) {
        List<JsonObject> rows = new ArrayList<>();
        Random random = new Random();
        Gson gson = new Gson();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.addProperty("f1_bool", random.nextBoolean());
            row.addProperty("f2_int8", random.nextInt((int) (Math.pow(2, 8))) - Math.pow(2, 7));
            row.addProperty("f3_int16", random.nextInt((int) (Math.pow(2, 16))) - Math.pow(2, 15));
            row.addProperty("f4_int32", random.nextInt());
            row.addProperty("f5_int64", random.nextLong());
            row.addProperty("f6_float", random.nextFloat());
            row.addProperty("f7_double", random.nextDouble());
            row.addProperty("f8_varchar", "str_" + i);

            List<Integer> intArray = new ArrayList<>();
            List<String> strArray = new ArrayList<>();
            int capacity = random.nextInt(5) + 5;
            for (int k = 0; k < capacity; k++) {
                intArray.add(i + k);
                strArray.add(String.format("string-%d-%d", i, k));
            }
            row.add("f9_array_int32", JsonUtils.toJsonTree(intArray).getAsJsonArray());
            row.add("f9_array_varchar", JsonUtils.toJsonTree(strArray).getAsJsonArray());

            JsonObject metadata = new JsonObject();
            metadata.addProperty("path", String.format("\\root/abc/path_%d", i));
            metadata.addProperty("size", i);
            if (i % 7 == 0) {
                metadata.addProperty("special", true);
            }
            metadata.add("flags", gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            row.add("f10_json", metadata);

            row.add("f11_floatvector", gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
            row.add("f12_binaryvector", gson.toJsonTree(CommonUtils.generateBinaryVector(VECTOR_DIM).array()));
            row.add("f13_float16vector", gson.toJsonTree(CommonUtils.generateFloat16Vector(VECTOR_DIM, false).array()));
            row.add("f14_bfloat16vector", gson.toJsonTree(CommonUtils.generateFloat16Vector(VECTOR_DIM, true).array()));
            row.add("f15_sparsefloatvector", gson.toJsonTree(CommonUtils.generateSparseVector()));
            rows.add(row);
        }
        return rows;
    }
}
