package com.milvus.connector;

import com.milvus.util.MilvusProxy;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;


public class MilvusTable extends AbstractTable {
    protected final MilvusProxy milvusProxy;
    protected final String dbName;
    protected final String collectionName;
    protected DescribeCollectionResp collectionDesc;
    protected Map<String, DescribeIndexResp> indexDesc = new HashMap<>();
    protected RelDataType relDataType;
    public static final String metaFieldPartition = "__partition__";
    public static final String metaFieldScore = "__score__";

    public MilvusTable(MilvusProxy milvusProxy, String collectionName) {
        this.milvusProxy = milvusProxy;
        this.collectionName = collectionName;
        this.dbName = milvusProxy.getDb();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        if (this.relDataType != null) {
            return this.relDataType;
        }
        this.collectionDesc = milvusProxy.getCollectionDesc(collectionName);
        RelDataTypeFactory.FieldInfoBuilder fieldInfoBuilder = relDataTypeFactory.builder();
        for (CreateCollectionReq.FieldSchema fieldSchema : collectionDesc.getCollectionSchema().getFieldSchemaList()) {
            String fieldName = fieldSchema.getName();
            DataType fieldType = fieldSchema.getDataType();
            this.indexDesc.put(fieldName, milvusProxy.getIndexDesc(collectionName, fieldName));
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
                    switch (fieldSchema.getElementType()) {
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
                            throw new RuntimeException("Unable to recognize Milvus element type: " + fieldSchema.getElementType().toString());
                    }
                    break;
                case JSON:
                    fieldInfoBuilder.add(fieldName, SqlTypeName.VARCHAR);
                    break;
                case BinaryVector:
                    fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.TINYINT), -1));
                    break;
                case FloatVector:
                    fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.FLOAT), -1));
                    break;
                case Float16Vector:
                    fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.TINYINT), -1));
                    break;
                case BFloat16Vector:
                    fieldInfoBuilder.add(fieldName, relDataTypeFactory.createArrayType(relDataTypeFactory.createSqlType(SqlTypeName.TINYINT), -1));
                    break;
                case SparseFloatVector:
                    fieldInfoBuilder.add(fieldName, relDataTypeFactory.createMapType(relDataTypeFactory.createSqlType(SqlTypeName.BIGINT), relDataTypeFactory.createSqlType(SqlTypeName.FLOAT)));
                    break;
                default:
                    throw new RuntimeException("Unable to recognize Milvus data type: " + fieldType.toString());
            }
        }
        fieldInfoBuilder.add(metaFieldPartition, SqlTypeName.VARCHAR);
        fieldInfoBuilder.add(metaFieldScore, SqlTypeName.FLOAT);
        this.relDataType = fieldInfoBuilder.build();
        return relDataType;
    }
}
