package com.milvus.connector;

import com.milvus.util.MilvusProxy;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;

import java.lang.reflect.Type;

public class MilvusTranslatableTable extends MilvusTable implements TranslatableTable, QueryableTable {
    public MilvusTranslatableTable(MilvusProxy milvusProxy, String collectionName) {
        super(milvusProxy, collectionName);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext toRelContext, RelOptTable relOptTable) {
        return new MilvusTableScan(toRelContext.getCluster(), relOptTable);
    }

    public Enumerable<Object[]> query(final DataContext root, MilvusPushDownParam milvusPushDownParam) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new MilvusEnumerator<>(MilvusTranslatableTable.this, milvusPushDownParam);
            }
        };
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schemaPlus, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }

    @Override
    public Expression getExpression(SchemaPlus schemaPlus, String tableName, Class clazz) {
        return Schemas.tableExpression(schemaPlus, getElementType(), tableName, clazz);
    }
}
