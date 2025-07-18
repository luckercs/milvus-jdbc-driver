package com.connector;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class MilvusSchemaFactory implements SchemaFactory {
    @Override
    public Schema create(SchemaPlus schemaPlus, String name, Map<String, Object> operand) {
        String milvusConnectURL = (String) operand.get("milvusConnectURL");
        String milvusConnectUsername = (String) operand.get("milvusConnectUsername");
        String milvusConnectPassword = (String) operand.get("milvusConnectPassword");
        String milvusConnectDatabase = (String) operand.get("milvusConnectDatabase");
        return new MilvusSchema(milvusConnectURL, milvusConnectUsername, milvusConnectPassword, milvusConnectDatabase);
    }
}
