package com.milvus.functions;

public class Ann {

    public static String annFuncName = "ann";
    public static String annsFuncName = "anns";

    public double ann(String vecCol, String vecList) {
        throw new UnsupportedOperationException("ANN function SQL must contain ORDER BY statement and LIMIT statement");
    }
    public double anns(String vecCol, String vecList, String searchParams) {
        throw new UnsupportedOperationException("ANNs function SQL must contain ORDER BY statement and LIMIT statement");
    }
}
