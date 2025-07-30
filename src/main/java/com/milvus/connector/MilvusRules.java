package com.milvus.connector;

public abstract class MilvusRules {
    private MilvusRules(){}

    public static final MilvusFilterTableScanRule FILTER_SCAN =
            MilvusFilterTableScanRule.Config.DEFAULT.toRule();
}
