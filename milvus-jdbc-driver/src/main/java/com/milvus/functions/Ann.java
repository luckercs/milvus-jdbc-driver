package com.milvus.functions;

public class Ann {

    public static String funcName = "ann";

    public double ann(String vecCol, String vecList) {
        throw new UnsupportedOperationException("ANN function should be pushed down to MilvusSource");
    }
}
