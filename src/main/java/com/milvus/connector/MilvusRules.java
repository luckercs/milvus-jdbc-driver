package com.milvus.connector;

public abstract class MilvusRules {
    private MilvusRules(){}

    public static final MilvusFilterTableScanRule FILTER_RULE =
            MilvusFilterTableScanRule.Config.DEFAULT.toRule();

    public static final MilvusSortTableScanRule SORT_RULE =
            MilvusSortTableScanRule.Config.DEFAULT.toRule();
}
