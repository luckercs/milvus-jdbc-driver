package com.milvus.connector;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.milvus.functions.AnnFunctionImpl;
import com.milvus.functions.FeatureGen;
import com.milvus.util.MilvusProxy;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;

import java.util.*;

public class MilvusSchema extends AbstractSchema {
    private final MilvusProxy milvusProxy;
    private final Multimap<String, Function> functionMap;

    public MilvusSchema(String uri, String user, String password, String db, int timeoutMs, boolean useSSL, int batchSize) {
        this.milvusProxy = new MilvusProxy(uri, user, password, db, timeoutMs, useSSL, batchSize);
        this.functionMap = ArrayListMultimap.create();
        this.functionMap.put("ann", ScalarFunctionImpl.create(AnnFunctionImpl.class, "processFloatArrays"));
        this.functionMap.put("gen_vector", ScalarFunctionImpl.create(FeatureGen.class, "gen_random_float_vectors_str"));
    }

    @Override
    protected Map<String, Table> getTableMap() {
        List<String> collectionNames = milvusProxy.getAllCollections();
        Map<String, Table> tableMaps = new LinkedHashMap<>();
        for (String collectionName : collectionNames) {
//            tableMaps.put(collectionName, new MilvusScannableTable(milvusProxy, collectionName));
//            tableMaps.put(collectionName, new MilvusFilterableTable(milvusProxy, collectionName));
            tableMaps.put(collectionName, new MilvusTranslatableTable(milvusProxy, collectionName));
        }
        return tableMaps;
    }

    @Override
    protected Multimap<String, Function> getFunctionMultimap() {
        return functionMap;
    }

}
