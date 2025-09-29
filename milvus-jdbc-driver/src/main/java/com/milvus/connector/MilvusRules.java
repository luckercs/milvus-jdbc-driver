package com.milvus.connector;

public abstract class MilvusRules {
    private MilvusRules() {
    }

    public static final MilvusFilterTableScanRule FILTER_RULE =
            MilvusFilterTableScanRule.Config.DEFAULT.toRule();

    public static final MilvusProjectTableScanRule PROJECT_RULE =
            MilvusProjectTableScanRule.Config.DEFAULT.toRule();

    public static final MilvusSortTableScanRule SORT_RULE =
            MilvusSortTableScanRule.Config.DEFAULT.toRule();

    public static final MilvusLimitTableScanRule LIMIT_RULE =
            MilvusLimitTableScanRule.Config.DEFAULT.toRule();

    public static final MilvusProjectAnnTableScanRule PROJECT_ANN_RULE =
            MilvusProjectAnnTableScanRule.Config.DEFAULT.toRule();
}
