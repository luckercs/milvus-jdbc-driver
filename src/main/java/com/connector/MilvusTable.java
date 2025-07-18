package com.connector;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.SearchResults;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.dml.QueryIteratorParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;


public class MilvusTable extends AbstractTable implements TranslatableTable, QueryableTable {
    private static final Logger LOG = LoggerFactory.getLogger(MilvusTable.class);
    private final String databaseName;
    private final String collectionName;
    private final MilvusSchema milvusSchema;

    private RelDataTypeFactory relDataTypeFactory;
    private List<String> fieldNames = new ArrayList<>();
    private List<DataType> fieldTypes = new ArrayList<>();
    private RelDataType fieldRelDataType;
    private Map<String, DataType> fieldNamesAndElementTypes = new HashMap<>();
    private Map<String, String> searchParams;

    public static final List<String> metaFieldNames = new ArrayList<>();
    public static final List<DataType> metaFieldTypes = new ArrayList<>();

    static {
        metaFieldNames.add("__search_vector_field");
        metaFieldTypes.add(DataType.VarChar);
        metaFieldNames.add("__search_vector");
        metaFieldTypes.add(DataType.VarChar);
        metaFieldNames.add("__search_metric_type");
        metaFieldTypes.add(DataType.VarChar);
        metaFieldNames.add("__search_params");
        metaFieldTypes.add(DataType.VarChar);
        metaFieldNames.add("__search_partitions");
        metaFieldTypes.add(DataType.VarChar);
        metaFieldNames.add("__search_expr");
        metaFieldTypes.add(DataType.VarChar);
        metaFieldNames.add("__search_topk");
        metaFieldTypes.add(DataType.VarChar);
        metaFieldNames.add("__search_is_vector_binary");
        metaFieldTypes.add(DataType.VarChar);
    }


    public MilvusTable(String databaseName, String collectionName, MilvusSchema milvusSchema) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.milvusSchema = milvusSchema;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final int fieldCount = relOptTable.getRowType().getFieldCount();
        final int[] fields = identityList(fieldCount);
        return new MilvusTableScan(context.getCluster(), context.getTableHints(), relOptTable, this, fields);
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<DataType> getFieldTypes() {
        return fieldTypes;
    }

    public Map<String, DataType> getFieldNamesAndElementTypes() {
        return fieldNamesAndElementTypes;
    }

    public RelDataType getFieldRelDataType() {
        return fieldRelDataType;
    }

    public Map<String, String> getSearchParams() {
        return searchParams;
    }

    public void setSearchParams(Map<String, String> searchParams) {
        this.searchParams = searchParams;
    }

    public void removeMetaField() {
        List<String> fieldNamesWithNoMeta = new ArrayList<>();
        List<DataType> fieldTypesWithNoMeta = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (!metaFieldNames.contains(fieldNames.get(i))) {
                fieldNamesWithNoMeta.add(fieldNames.get(i));
                fieldTypesWithNoMeta.add(fieldTypes.get(i));
            }
        }

        RelDataTypeFactory.FieldInfoBuilder fieldRelDataTypeWithNoMetaBuilder = relDataTypeFactory.builder();
        List<RelDataTypeField> fieldList = fieldRelDataType.getFieldList();
        for (RelDataTypeField relDataTypeField : fieldList) {
            if (!metaFieldNames.contains(relDataTypeField.getName())) {
                fieldRelDataTypeWithNoMetaBuilder.add(relDataTypeField);
            }
        }
        RelDataType fieldRelDataTypeWithNoMeta = fieldRelDataTypeWithNoMetaBuilder.build();

        this.fieldNames = fieldNamesWithNoMeta;
        this.fieldTypes = fieldTypesWithNoMeta;
        this.fieldRelDataType = fieldRelDataTypeWithNoMeta;
    }

    public RelDataType getRowType() {
        return fieldRelDataType;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        if (fieldRelDataType != null) {
            return fieldRelDataType;
        }
        this.relDataTypeFactory = relDataTypeFactory;
        RelDataTypeFactory.FieldInfoBuilder fieldInfoBuilder = relDataTypeFactory.builder();
        MilvusServiceClient milvusClient = milvusSchema.getMilvusClient();
        DescribeCollectionParam describeCollectionParam = DescribeCollectionParam.newBuilder().withDatabaseName(databaseName).withCollectionName(collectionName).build();
        R<DescribeCollectionResponse> describeCollectionResponseR = milvusClient.describeCollection(describeCollectionParam);
        milvusSchema.handleMilvusResponseStatus(describeCollectionResponseR);
        milvusSchema.closeMilvusClient(milvusClient);
        List<FieldSchema> fieldsList = describeCollectionResponseR.getData().getSchema().getFieldsList();
        for (FieldSchema fieldSchema : fieldsList) {
            String fieldName = fieldSchema.getName();
            DataType fieldType = fieldSchema.getDataType();
            fieldNames.add(fieldName);
            fieldTypes.add(fieldType);
            if (fieldType == DataType.Array) {
                fieldNamesAndElementTypes.put(fieldName, fieldSchema.getElementType());
            }
            switch (fieldType) {
                case Bool:
                    fieldInfoBuilder.add(fieldName, SqlTypeName.BOOLEAN);
                    break;
                case Int8:
                    fieldInfoBuilder.add(fieldName, SqlTypeName.TINYINT);
                    break;
                case Int16:
                    fieldInfoBuilder.add(fieldName, SqlTypeName.SMALLINT);
                    break;
                case Int32:
                    fieldInfoBuilder.add(fieldName, SqlTypeName.INTEGER);
                    break;
                case Int64:
                    fieldInfoBuilder.add(fieldName, SqlTypeName.BIGINT);
                    break;
                case Float:
                    fieldInfoBuilder.add(fieldName, SqlTypeName.FLOAT);
                    break;
                case Double:
                    fieldInfoBuilder.add(fieldName, SqlTypeName.DOUBLE);
                    break;
                case String:
                    fieldInfoBuilder.add(fieldName, SqlTypeName.VARCHAR);
                    break;
                case VarChar:
                    fieldInfoBuilder.add(fieldName, SqlTypeName.VARCHAR);
                    break;
                case Array:
                    switch (fieldNamesAndElementTypes.get(fieldName)) {
                        case Bool:
                            fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.BOOLEAN), -1));
                            break;
                        case Int8:
                            fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.TINYINT), -1));
                            break;
                        case Int16:
                            fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.SMALLINT), -1));
                            break;
                        case Int32:
                            fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.INTEGER), -1));
                            break;
                        case Int64:
                            fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.BIGINT), -1));
                            break;
                        case Float:
                            fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.FLOAT), -1));
                            break;
                        case Double:
                            fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.DOUBLE), -1));
                            break;
                        case String:
                            fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR), -1));
                            break;
                        case VarChar:
                            fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR), -1));
                            break;
                        default:
                            fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR), -1));
                    }
                    break;
                case JSON:
                    fieldInfoBuilder.add(fieldName, relDataTypeFactory.createJavaType(JSONObject.class));
                    break;
                case BinaryVector:
                    fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.TINYINT), -1));
                    break;
                case FloatVector:
                    fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.FLOAT), -1));
                    break;
                default:
                    throw new RuntimeException("Unable to recognize Milvus data type: " + fieldType.toString());
            }
        }

        for (int i = 0; i < metaFieldNames.size(); i++) {
            fieldNames.add(metaFieldNames.get(i));
            fieldTypes.add(metaFieldTypes.get(i));
            fieldInfoBuilder.add(metaFieldNames.get(i), SqlTypeName.VARCHAR);
        }

        fieldRelDataType = fieldInfoBuilder.build();

        return fieldRelDataType;
    }

    public Enumerable<Object[]> project(final DataContext root, final int[] fields) {
        List<String> outFieldNames = new ArrayList<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (!metaFieldNames.contains(fieldNames.get(i))) {
                outFieldNames.add(fieldNames.get(i));
            }
        }
        if (searchParams == null) {
            LOG.info("come here");
            final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
            MilvusServiceClient milvusClient = milvusSchema.getMilvusClient();
            QueryIteratorParam queryIteratorParam = QueryIteratorParam.newBuilder()
                    .withDatabaseName(databaseName)
                    .withCollectionName(collectionName)
                    .withOutFields(outFieldNames)
                    .withBatchSize(100L)
                    .build();
            R<QueryIterator> queryIteratorR = milvusClient.queryIterator(queryIteratorParam);
            milvusSchema.handleMilvusResponseStatus(queryIteratorR);
            QueryIterator queryIterator = queryIteratorR.getData();
            return new AbstractEnumerable<Object[]>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    return new MilvusEnumerator(queryIterator, fieldNames, fieldTypes, milvusSchema, milvusClient);
                }
            };
        } else {
            LOG.info("come search here");
            final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
            String vecFieldName = searchParams.get("__search_vector_field");
            int topk = Integer.parseInt(searchParams.getOrDefault("__search_topk", "10"));
            String params = searchParams.get("__search_params");
            boolean vectorListIsByte = Boolean.parseBoolean(searchParams.getOrDefault("__search_is_vector_binary", "false"));
            String vectorList = searchParams.get("__search_vector");
            String partitionNames = searchParams.getOrDefault("__search_partitions", null);
            String metricType = searchParams.get("__search_metric_type");
            String expr = searchParams.getOrDefault("__search_expr", null);

//            List<String> outputField = new ArrayList<>();
//            List<DataType> outputTypes = new ArrayList<>();
//            for (int i = 0; i < fields.length; i++) {
//                outputField.add(fieldNames.get(i));
//                outputTypes.add(fieldTypes.get(i));
//            }


            MilvusServiceClient milvusClient = milvusSchema.getMilvusClient();
            SearchParam.Builder searchParamBuilder = SearchParam.newBuilder()
                    .withOutFields(outFieldNames)
                    .withDatabaseName(databaseName)
                    .withCollectionName(collectionName)
                    .withVectorFieldName(vecFieldName)
                    .withTopK(topk)   // Top K: max 16,384
                    .withConsistencyLevel(ConsistencyLevelEnum.STRONG);

            if (params != null && !params.equals("")) {
                searchParamBuilder.withParams(params);
            }
            if (!vectorListIsByte) {
                searchParamBuilder.withVectors(convertStringToListListFloat(vectorList));
            } else {
                searchParamBuilder.withVectors(convertListListByteToListByteBuffer(convertStringToListListByte(vectorList)));
            }
            if (partitionNames != null && !partitionNames.equals("")) {
                searchParamBuilder.withPartitionNames(Arrays.asList(trimArrayElements(partitionNames.split(","))));
            }
            if (metricType != null && !metricType.equals("")) {
                switch (metricType.toUpperCase()) {
                    case "L2":
                        searchParamBuilder.withMetricType(MetricType.L2);
                        break;
                    case "IP":
                        searchParamBuilder.withMetricType(MetricType.IP);
                        break;
                    case "COSINE":
                        searchParamBuilder.withMetricType(MetricType.COSINE);
                        break;
                    case "HAMMING":
                        searchParamBuilder.withMetricType(MetricType.HAMMING);
                        break;
                    case "JACCARD":
                        searchParamBuilder.withMetricType(MetricType.JACCARD);
                        break;
                    default:
                        searchParamBuilder.withMetricType(MetricType.None);
                }
            }
            if (expr != null && expr != "") {
                searchParamBuilder.withExpr(expr);
            }

            SearchParam searchParam = searchParamBuilder.build();
            R<SearchResults> searchR = milvusClient.search(searchParam);
            milvusSchema.handleMilvusResponseStatus(searchR);
            SearchResultsWrapper searchResultsWrapper = new SearchResultsWrapper(searchR.getData().getResults());
            List<QueryResultsWrapper.RowRecord> rowRecords = searchResultsWrapper.getRowRecords(0);
            return new AbstractEnumerable<Object[]>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    return new MilvusSearchEnumerator(rowRecords, fieldNames, fieldTypes, milvusSchema, milvusClient);
                }
            };
        }

    }

//    public Enumerable<Object> project2(final DataContext root, final int[] fields, HashMap<String, String> searchParams) {
//
//    }

    private List<List<Float>> convertStringToListListFloat(String str) {
        Gson gson = new Gson();
        Type listType = new TypeToken<List<List<Float>>>() {
        }.getType();
        List<List<Float>> listOfListOfFloat = gson.fromJson(str, listType);
        return listOfListOfFloat;
    }

    private List<ByteBuffer> convertListListByteToListByteBuffer(List<List<Byte>> bytesList) {
        List<ByteBuffer> vectors = new ArrayList<>();
        for (List<Byte> bytes : bytesList) {
            ByteBuffer vector = ByteBuffer.allocate(bytes.size());
            for (Byte b : bytes) {
                vector.put(b);
            }
            vectors.add(vector);
        }
        return vectors;
    }

    private List<List<Byte>> convertStringToListListByte(String str) {
        Gson gson = new Gson();
        Type listType = new TypeToken<List<List<Byte>>>() {
        }.getType();
        List<List<Byte>> listOfListOfFloat = gson.fromJson(str, listType);
        return listOfListOfFloat;
    }

    private String[] trimArrayElements(String[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] = array[i].trim();
        }
        return array;
    }

    private int[] identityList(int n) {
        int[] integers = new int[n];
        for (int i = 0; i < n; i++) {
            integers[i] = i;
        }
        return integers;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }
}
