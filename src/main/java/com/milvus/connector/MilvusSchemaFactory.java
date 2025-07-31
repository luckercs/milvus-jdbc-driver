package com.milvus.connector;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class MilvusSchemaFactory implements SchemaFactory {

//    @Override
//    public Schema create(SchemaPlus schemaPlus, String name, Map<String, Object> operand) {
//        Properties milvusProps = filterDataSourceProps(operand, "milvus");
//        SchemaPlus rootSchema = schemaPlus.add(name, new AbstractSchema() {});
//        Schema milvusSchema = new MilvusSchema(
//                milvusProps.getProperty(MilvusSchemaOptions.URL),
//                milvusProps.getProperty(MilvusSchemaOptions.UserName),
//                milvusProps.getProperty(MilvusSchemaOptions.PassWord),
//                milvusProps.getProperty(MilvusSchemaOptions.DB));
//        rootSchema.add("milvus", milvusSchema);
//        rootSchema.add("gen_vector", ScalarFunctionImpl.create(FeatureGen.class, "gen_random_float_vectors_str"));
//
//        return rootSchema;
//    }

    @Override
    public Schema create(SchemaPlus schemaPlus, String name, Map<String, Object> operand) {
        Schema milvusSchema = new MilvusSchema(
                (String) operand.get(MilvusSchemaOptions.URL),
                (String) operand.get(MilvusSchemaOptions.UserName),
                (String) operand.get(MilvusSchemaOptions.PassWord),
                (String) operand.get(MilvusSchemaOptions.DB));
        return milvusSchema;
    }

//    @Override
//    public Schema create(SchemaPlus schemaPlus, String name, Map<String, Object> operand) {
//        String milvusConnectURL = (String) operand.get("milvusConnectURL");
//        String milvusConnectUsername = (String) operand.get("milvusConnectUsername");
//        String milvusConnectPassword = (String) operand.get("milvusConnectPassword");
//        String milvusConnectDatabase = (String) operand.get("milvusConnectDatabase");
//        return new MilvusSchema(milvusConnectURL, milvusConnectUsername, milvusConnectPassword, milvusConnectDatabase);
//}


}


