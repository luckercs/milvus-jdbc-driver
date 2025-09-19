package com.milvus.connector;

import com.milvus.util.MilvusProxy;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.*;

public class MilvusSchema extends AbstractSchema {
    private final MilvusProxy milvusProxy;

    public MilvusSchema(String uri, String user, String password, String db, int timeoutMs, boolean useSSL, int batchSize) {
        this.milvusProxy = new MilvusProxy(uri, user, password, db, timeoutMs, useSSL, batchSize);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        List<String> collectionNames = milvusProxy.getAllCollections();
        Map<String, Table> tableMaps = new LinkedHashMap<>();
        for (String collectionName : collectionNames) {
//            tableMaps.put(collectionName, new MilvusScannableTable(milvusProxy, collectionName));
//            tableMaps.put(collectionName, new MilvusFilterableTable(milvusProxy, collectionName));
            MilvusTranslatableTable milvusTranslatableTable = new MilvusTranslatableTable(milvusProxy, collectionName);

            tableMaps.put(collectionName, milvusTranslatableTable);
        }
        return tableMaps;
    }
}
