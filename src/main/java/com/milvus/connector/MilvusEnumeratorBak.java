package com.milvus.connector;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;


class MilvusEnumeratorBak<E> implements Enumerator<E> {
    private static final Logger LOG = LoggerFactory.getLogger(MilvusEnumeratorBak.class);

    private QueryIterator queryIterator;
    private List<QueryResultsWrapper.RowRecord> pageRowRecord;
    private List<DataType> fieldTypes;
    private List<String> fieldNames;
    private final MilvusSchema milvusSchema;

    private final MilvusServiceClient milvusClient;

    private int pageIndex = 0;
    private int pageRowNum = 0;
    private int pageCount;
    private int filedNum;

    private E current;

    public MilvusEnumeratorBak(QueryIterator queryIterator, List<String> fieldNames, List<DataType> fieldTypes, MilvusSchema milvusSchema, MilvusServiceClient milvusClient) {
        this.queryIterator = queryIterator;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.filedNum = fieldNames.size();
        this.milvusSchema = milvusSchema;
        this.milvusClient = milvusClient;
    }

    @Override
    public E current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        while (true) {
            if (pageRowRecord == null) {
                pageRowRecord = queryIterator.next();
                if (pageRowRecord.isEmpty()) {
                    return false;
                }
                pageCount = pageRowRecord.size();
                pageIndex = 0;
            }

            if (pageIndex < pageCount) {
                pageRowNum = pageIndex;

                Object[] res = new Object[filedNum];
                Map<String, Object> fieldValues = pageRowRecord.get(pageRowNum).getFieldValues();

                for (int i = 0; i < fieldNames.size(); i++) {
                    if (fieldValues.get(fieldNames.get(i)) != null) {
                        res[i] = convert(fieldValues.get(fieldNames.get(i)), fieldTypes.get(i));
                    } else {
                        res[i] = null;
                    }
                }

                current = (E) res;
                pageIndex++;
                return true;
            } else {
                pageRowRecord = null;
                LOG.info("this page over");
            }
        }
    }

    @Override
    public void reset() {
        pageRowRecord = null;
        pageIndex = 0;
    }

    @Override
    public void close() {
        queryIterator.close();
        milvusSchema.closeMilvusClient(milvusClient);
        LOG.info("close done");
    }

    private Object convert(Object value, DataType dataType) {
        switch (dataType) {
            case Bool:
                return (Boolean) value;
            case Int8:
                return Byte.parseByte(value.toString());
            case Int16:
                return Short.parseShort(value.toString());
            case Int32:
                return (Integer) value;
            case Int64:
                return (Long) value;
            case Float:
                return (Float) value;
            case Double:
                return (Double) value;
            case String:
                return (String) value;
            case VarChar:
                return (String) value;
            case Array:
                return ((List<E>) value).toArray();
            case JSON:
                return (JSONObject) value;
            case BinaryVector:
                return ((ByteBuffer) value).array();
            case FloatVector:
                return ((List<E>) value).toArray();
            default:
                return (String) value;
        }
    }
}

