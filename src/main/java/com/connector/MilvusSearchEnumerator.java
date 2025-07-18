package com.connector;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.response.QueryResultsWrapper;
import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;


class MilvusSearchEnumerator<E> implements Enumerator<E> {
    private static final Logger LOG = LoggerFactory.getLogger(MilvusSearchEnumerator.class);

    private List<QueryResultsWrapper.RowRecord> rowRecords;
    private List<DataType> fieldTypes;
    private List<String> fieldNames;

    private final MilvusSchema milvusSchema;

    private final MilvusServiceClient milvusClient;
    private int index;
    private int rowNum;
    private int count;
    private int filedNum;

    private E current;


    public MilvusSearchEnumerator(List<QueryResultsWrapper.RowRecord> rowRecords, List<String> fieldNames, List<DataType> fieldTypes, MilvusSchema milvusSchema, MilvusServiceClient milvusClient) {
        this.rowRecords = rowRecords;
        this.count = rowRecords.size();
        this.filedNum = fieldNames.size();
        this.fieldTypes = fieldTypes;
        this.fieldNames = fieldNames;
        this.milvusSchema = milvusSchema;
        this.milvusClient = milvusClient;
    }

    @Override
    public E current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        if (index < count) {
            rowNum = index;

            Object[] res = new Object[filedNum];
            Map<String, Object> fieldValues = rowRecords.get(rowNum).getFieldValues();

            for (int i = 0; i < fieldNames.size(); i++) {
                if (fieldValues.get(fieldNames.get(i)) != null) {
                    res[i] = convert(fieldValues.get(fieldNames.get(i)), fieldTypes.get(i));
                } else {
                    res[i] = null;
                }

            }

            current = (E) res;
            index++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void reset() {
        index = 0;
    }

    @Override
    public void close() {
        milvusSchema.closeMilvusClient(milvusClient);
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
                return  ((ByteBuffer) value).array();
            case FloatVector:
                return ((List<E>) value).toArray();
            default:
                return (String) value;
        }
    }
}

