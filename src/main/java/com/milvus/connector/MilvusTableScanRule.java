package com.milvus.connector;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;

public class MilvusTableScanRule extends RelRule<MilvusTableScanRule.Config> {
    protected MilvusTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {

    }

    public interface Config extends RelRule.Config {

    }
}
