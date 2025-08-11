package com.milvus.connector;

import com.google.protobuf.ProtocolStringList;
import com.milvus.util.MilvusProxy;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.ShowCollectionsParam;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 所有表的集合实现
 */

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
            tableMaps.put(milvusProxy.getDb() + "." + collectionName, new MilvusTable(milvusProxy, collectionName));
        }
        return tableMaps;
    }

    public void handleMilvusResponseStatus(R<?> r) {
        if (r.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(r.getMessage());
        }
    }
}
