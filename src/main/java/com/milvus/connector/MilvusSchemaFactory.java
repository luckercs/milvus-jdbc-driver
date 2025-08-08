package com.milvus.connector;

import com.milvus.options.MilvusSchemaOptions;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class MilvusSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus schemaPlus, String name, Map<String, Object> operand) {
        return new MilvusSchema(
                ((String) operand.getOrDefault(MilvusSchemaOptions.URI, MilvusSchemaOptions.getStringDefaultValue(MilvusSchemaOptions.URI))).trim(),
                ((String) operand.getOrDefault(MilvusSchemaOptions.User, MilvusSchemaOptions.getStringDefaultValue(MilvusSchemaOptions.User))).trim(),
                ((String) operand.getOrDefault(MilvusSchemaOptions.PassWord, MilvusSchemaOptions.getStringDefaultValue(MilvusSchemaOptions.PassWord))).trim(),
                ((String) operand.getOrDefault(MilvusSchemaOptions.DB, MilvusSchemaOptions.getStringDefaultValue(MilvusSchemaOptions.DB))).trim(),
                (Integer) operand.getOrDefault(MilvusSchemaOptions.TimeOutMs, MilvusSchemaOptions.getIntDefaultValue(MilvusSchemaOptions.TimeOutMs)),
                (Boolean) operand.getOrDefault(MilvusSchemaOptions.UseSSL, MilvusSchemaOptions.getBoolDefaultValue(MilvusSchemaOptions.UseSSL))
        );
    }
}


