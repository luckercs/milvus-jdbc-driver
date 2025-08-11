package com.milvus.connector;

import com.milvus.util.MilvusProxy;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.ScannableTable;
import org.checkerframework.checker.nullness.qual.Nullable;

public class MilvusScannableTable extends MilvusTable implements ScannableTable {

    public MilvusScannableTable(MilvusProxy milvusProxy, String collectionName) {
        super(milvusProxy, collectionName);
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext dataContext) {

        return new AbstractEnumerable<@Nullable Object[]>() {

            @Override
            public Enumerator<@Nullable Object[]> enumerator() {
                return new MilvusEnumerator<>(this);
            }
        }
    }

    @Override
    public String toString() {
        return "MilvusScannableTable";
    }
}
