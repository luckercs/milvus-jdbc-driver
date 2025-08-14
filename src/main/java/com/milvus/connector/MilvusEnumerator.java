package com.milvus.connector;

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;


class MilvusEnumerator<E> implements Enumerator<Object[]> {
    private static final Logger LOG = LoggerFactory.getLogger(MilvusEnumerator.class);

    private MilvusClientV2 milvusClient;
    private QueryIterator queryIterator;
    private MilvusTable milvusTable;
    private String filterExpr;

    private Object[] currentRow;
    private List<QueryResultsWrapper.RowRecord> currentPage;
    private int currentRowIndex;

    public MilvusEnumerator(MilvusTable milvusTable, String filterExpr) {
        this.milvusTable = milvusTable;
        this.milvusClient = milvusTable.milvusProxy.getClient();
        this.filterExpr = filterExpr;
        initQueryIteratorReq();
    }

    @Override
    public Object[] current() {
        return currentRow;
    }

    @Override
    public boolean moveNext() {
        if (currentPage == null || currentRowIndex >= currentPage.size()) {
            currentRowIndex = 0;
            List<QueryResultsWrapper.RowRecord> rowRecords = queryIterator.next();
            LOG.info("milvus queryIterator Current row count: " + rowRecords.size());
            if (rowRecords.isEmpty()) {
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
        initQueryIteratorReq();
    }

    @Override
    public void close() {
        milvusTable.milvusProxy.closeQueryIterator(queryIterator);
        milvusTable.milvusProxy.closeClient(milvusClient);
    }

    private void initQueryIteratorReq() {
        LOG.info("milvusClient.queryIterator, filterExpr: " + filterExpr);
        QueryIteratorReq queryIteratorReq = QueryIteratorReq.builder()
                .databaseName(milvusTable.dbName)
                .collectionName(milvusTable.collectionName)
                .outputFields(milvusTable.collectionDesc.getFieldNames())
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .batchSize(milvusTable.milvusProxy.getBatchSize())
                .expr(filterExpr)
                .build();
        this.queryIterator = milvusClient.queryIterator(queryIteratorReq);
    }

    private Object[] convertRowRecord(QueryResultsWrapper.RowRecord rowRecord) {
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = milvusTable.collectionDesc.getCollectionSchema().getFieldSchemaList();
        ArrayList<Object> resList = new ArrayList<>();
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
        return resList.toArray();
    }
}

