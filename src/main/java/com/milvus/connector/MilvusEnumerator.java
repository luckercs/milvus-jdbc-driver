package com.milvus.connector;

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;


class MilvusEnumerator<E> implements Enumerator<Object[]> {
    private static final Logger LOG = LoggerFactory.getLogger(MilvusEnumerator.class);

    private MilvusClientV2 milvusClient;
    private QueryIterator queryIterator;
    private SearchIterator searchIterator;
    private MilvusTable milvusTable;
    private MilvusPushDownParam milvusPushDownParam;

    private Object[] currentRow;
    private List<QueryResultsWrapper.RowRecord> currentPage;
    private int currentRowIndex;


    public MilvusEnumerator(MilvusTable milvusTable, MilvusPushDownParam milvusPushDownParam) {
        this.milvusTable = milvusTable;
        this.milvusClient = milvusTable.milvusProxy.getClient();
        this.milvusPushDownParam = milvusPushDownParam;
        if (milvusPushDownParam.isSearchQuery()) {
            initSearchIteratorReq();
        } else {
            initQueryIteratorReq();
        }
    }

    @Override
    public Object[] current() {
        return currentRow;
    }

    @Override
    public boolean moveNext() {
        if (currentPage == null || currentRowIndex >= currentPage.size()) {
            currentRowIndex = 0;
            List<QueryResultsWrapper.RowRecord> rowRecords;
            if (milvusPushDownParam.isSearchQuery()) {
                rowRecords = searchIterator.next();
            } else {
                rowRecords = queryIterator.next();
            }
            LOG.info("milvus queryIterator next page rowcount: " + rowRecords.size());
            if (rowRecords.isEmpty()) {
                close();
                return false;
            } else {
                currentPage = rowRecords;
            }
        }
        QueryResultsWrapper.RowRecord rowRecord = currentPage.get(currentRowIndex++);
        currentRow = convertRowRecord(rowRecord);
        return true;
    }

    @Override
    public void reset() {
        LOG.info("milvusClient.queryIterator reset");
        if (milvusPushDownParam.isSearchQuery()) {
            initSearchIteratorReq();
        } else {
            initQueryIteratorReq();
        }
    }

    @Override
    public void close() {
        LOG.info("milvusClient.queryIterator closed");
        if (milvusPushDownParam.isSearchQuery()) {
            milvusTable.milvusProxy.closeSearchIterator(searchIterator);
        } else {
            milvusTable.milvusProxy.closeQueryIterator(queryIterator);
        }
        milvusTable.milvusProxy.closeClient(milvusClient);
    }

    private void initQueryIteratorReq() {
        QueryIteratorReq.QueryIteratorReqBuilder<?, ?> queryIteratorReqBuilder = QueryIteratorReq.builder()
                .databaseName(milvusTable.dbName)
                .collectionName(milvusTable.collectionName)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .batchSize(milvusTable.milvusProxy.getBatchSize());

        if (milvusPushDownParam != null && milvusPushDownParam.getFilterExpr() != null && !milvusPushDownParam.getFilterExpr().equals("")) {
            queryIteratorReqBuilder.expr(milvusPushDownParam.getFilterExpr());
            LOG.info("milvus query PushDownParam filterExpr: " + milvusPushDownParam.getFilterExpr());
        }
        if (milvusPushDownParam != null && milvusPushDownParam.getPartitionNames() != null && !milvusPushDownParam.getPartitionNames().isEmpty()) {
            queryIteratorReqBuilder.partitionNames(milvusPushDownParam.getPartitionNames());
            LOG.info("milvus query PushDownParam partitionNames: " + milvusPushDownParam.getPartitionNames());
        }
        if (milvusPushDownParam != null && milvusPushDownParam.getLimit() != null && milvusPushDownParam.getLimit() > 0) {
            queryIteratorReqBuilder.limit(milvusPushDownParam.getLimit());
            LOG.info("milvus query PushDownParam limit: " + milvusPushDownParam.getLimit());
        }
        if (milvusPushDownParam != null && milvusPushDownParam.getOffset() != null && milvusPushDownParam.getOffset() > 0) {
            queryIteratorReqBuilder.offset(milvusPushDownParam.getOffset());
            LOG.info("milvus query PushDownParam offset: " + milvusPushDownParam.getOffset());
        }
        if (milvusPushDownParam != null && milvusPushDownParam.getOutputFields() != null && !milvusPushDownParam.getOutputFields().isEmpty()) {
            queryIteratorReqBuilder.outputFields(milvusPushDownParam.getOutputFields());
            LOG.info("milvus query PushDownParam outputFields: " + milvusPushDownParam.getOutputFields());
        } else {
            queryIteratorReqBuilder.outputFields(milvusTable.collectionDesc.getFieldNames());
        }
        QueryIteratorReq queryIteratorReq = queryIteratorReqBuilder.build();
        this.queryIterator = milvusClient.queryIterator(queryIteratorReq);
    }

    private void initSearchIteratorReq() {
        List<BaseVector> queryVectors = Collections.singletonList(new FloatVec(stringToFloatList(milvusPushDownParam.getSearchVec())));
        IndexParam.MetricType metricType = milvusTable.indexDesc.get(milvusPushDownParam.getSearchVecColName()).getIndexDescriptions().get(0).getMetricType();
        SearchIteratorReq.SearchIteratorReqBuilder<?, ?> searchIteratorReqBuilder = SearchIteratorReq.builder()
                .databaseName(milvusTable.dbName)
                .collectionName(milvusTable.collectionName)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .vectorFieldName(milvusPushDownParam.getSearchVecColName())
                .metricType(metricType)
                .vectors(queryVectors)
                .batchSize(milvusTable.milvusProxy.getBatchSize());

        if (milvusPushDownParam != null && milvusPushDownParam.getFilterExpr() != null && !milvusPushDownParam.getFilterExpr().equals("")) {
            searchIteratorReqBuilder.expr(milvusPushDownParam.getFilterExpr());
            LOG.info("milvus search PushDownParam filterExpr: " + milvusPushDownParam.getFilterExpr());
        }
        if (milvusPushDownParam != null && milvusPushDownParam.getPartitionNames() != null && !milvusPushDownParam.getPartitionNames().isEmpty()) {
            searchIteratorReqBuilder.partitionNames(milvusPushDownParam.getPartitionNames());
            LOG.info("milvus search PushDownParam partitionNames: " + milvusPushDownParam.getPartitionNames());
        }
        if (milvusPushDownParam != null && milvusPushDownParam.getLimit() != null && milvusPushDownParam.getLimit() > 0) {
            searchIteratorReqBuilder.limit(milvusPushDownParam.getLimit());
            LOG.info("milvus search PushDownParam limit: " + milvusPushDownParam.getLimit());
        }
        if (milvusPushDownParam != null && milvusPushDownParam.getOutputFields() != null && !milvusPushDownParam.getOutputFields().isEmpty()) {
            searchIteratorReqBuilder.outputFields(milvusPushDownParam.getOutputFields());
            LOG.info("milvus search PushDownParam outputFields: " + milvusPushDownParam.getOutputFields());
        } else {
            searchIteratorReqBuilder.outputFields(milvusTable.collectionDesc.getFieldNames());
        }
        LOG.info("milvus search queryVector=" + stringToFloatList(milvusPushDownParam.getSearchVec()));
        SearchIteratorReq searchIteratorReq = searchIteratorReqBuilder.build();
        this.searchIterator = milvusClient.searchIterator(searchIteratorReq);
    }

    private Object[] convertRowRecord(QueryResultsWrapper.RowRecord rowRecord) {
        ArrayList<Object> resList = new ArrayList<>();
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = milvusTable.collectionDesc.getCollectionSchema().getFieldSchemaList();
        for (CreateCollectionReq.FieldSchema fieldSchema : fieldSchemaList) {
            Object value = rowRecord.getFieldValues().get(fieldSchema.getName());
            switch (fieldSchema.getDataType()) {
                case Bool:
                    resList.add((Boolean) value);
                    break;
                case Int8:
                    resList.add(((Integer) value).byteValue());
                    break;
                case Int16:
                    resList.add(((Integer) value).shortValue());
                    break;
                case Int32:
                    resList.add((Integer) value);
                    break;
                case Int64:
                    resList.add((Long) value);
                    break;
                case Float:
                    resList.add((Float) value);
                    break;
                case Double:
                    resList.add((Double) value);
                    break;
                case String:
                    resList.add((String) value);
                    break;
                case VarChar:
                    resList.add((String) value);
                    break;
                case JSON:
                    resList.add(value.toString());
                    break;
                case Array:
                    resList.add(((List) value).toArray());
                    break;
                case BinaryVector:
                    ((ByteBuffer) value).flip();
                    byte[] byteArrayBinary = new byte[((ByteBuffer) value).remaining()];
                    ((ByteBuffer) value).get(byteArrayBinary);
                    resList.add(byteArrayBinary);
                    break;
                case FloatVector:
                    resList.add(((List) value).toArray());
                    break;
                case Float16Vector:
                    ((ByteBuffer) value).flip();
                    byte[] byteArrayFloat16 = new byte[((ByteBuffer) value).remaining()];
                    ((ByteBuffer) value).get(byteArrayFloat16);
                    resList.add(byteArrayFloat16);
                    break;
                case BFloat16Vector:
                    ((ByteBuffer) value).flip();
                    byte[] byteArrayBFloat16 = new byte[((ByteBuffer) value).remaining()];
                    ((ByteBuffer) value).get(byteArrayBFloat16);
                    resList.add(byteArrayBFloat16);
                    break;
                case SparseFloatVector:
                    resList.add((TreeMap<Long, Float>) value);
                    break;
                default:
                    throw new RuntimeException("Unsupported data type: " + fieldSchema.getDataType());

            }
        }
        // add default score
        if (rowRecord.contains(MilvusTable.milvusSearchFieldScore)) {
            Object value = rowRecord.get(MilvusTable.milvusSearchFieldScore);
            resList.add(((Float)value).doubleValue());
        } else {
            resList.add(0.0);
        }
        // add default partitionName
        resList.add("");
        return resList.toArray();
    }

    public static List<Float> stringToFloatList(String input) {
        List<Float> floatList = new ArrayList<>();
        input = input.trim();
        if (!input.startsWith("[") || !input.endsWith("]")) {
            throw new IllegalArgumentException("input data format error，eg: [0.1, 0.2, 0.3...]");
        }
        String content = input.substring(1, input.length() - 1).trim();
        if (content.isEmpty()) {
            return floatList;
        }
        String[] elements;
        if (content.contains(",")) {
            elements = content.split(",");
        } else {
            elements = content.split("\\s+");
        }
        for (String element : elements) {
            String trimmedElement = element.trim();
            if (!trimmedElement.isEmpty()) {
                try {
                    floatList.add(Float.parseFloat(trimmedElement));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(trimmedElement + "can not change to Float", e);
                }
            }
        }
        return floatList;
    }
}

