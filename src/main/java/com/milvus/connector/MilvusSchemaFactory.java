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

    @Override
    public Schema create(SchemaPlus schemaPlus, String name, Map<String, Object> operand) {
        Properties milvusProps = filterDataSourceProps(operand, "milvus");
        SchemaPlus rootSchema = schemaPlus.add(name, new AbstractSchema() {});
        Schema milvusSchema = new MilvusSchema(
                milvusProps.getProperty(MilvusSchemaOptions.URL),
                milvusProps.getProperty(MilvusSchemaOptions.UserName),
                milvusProps.getProperty(MilvusSchemaOptions.PassWord),
                milvusProps.getProperty(MilvusSchemaOptions.DB));
        rootSchema.add("milvus", milvusSchema);
        rootSchema.add("gen_vector", ScalarFunctionImpl.create(FeatureGen.class, "gen_random_float_vectors_str"));

        return rootSchema;
    }

    public static Properties filterDataSourceProps(Map<String, Object> originalProperties, String dataSourceName) {
        Properties resProperties = new Properties();

        if (originalProperties == null) {
            return resProperties;
        }

        Set<String> keys = originalProperties.keySet();
        for (String key : keys) {
            if (key != null && key.startsWith(dataSourceName + ".")) {
                String value = (String) originalProperties.get(key);
                resProperties.setProperty(key.replace(dataSourceName + ".", ""), value);
            }
        }
        return resProperties;
    }
}


