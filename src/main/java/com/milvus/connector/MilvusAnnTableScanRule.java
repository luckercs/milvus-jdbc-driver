package com.milvus.connector;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.immutables.value.Value;

@Value.Enclosing
public class MilvusAnnTableScanRule extends RelRule<MilvusAnnTableScanRule.Config> {
    protected MilvusAnnTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {

    }


    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT = ImmutableMilvusAnnTableScanRule.Config.builder().build()
                .withOperandSupplier(sort -> sort.operand(LogicalSort.class)
                        .inputs(project -> project.operand(LogicalProject.class)
                                .inputs(scan -> scan.operand(MilvusTableScan.class).noInputs())));

        @Override
        default MilvusAnnTableScanRule toRule() {
            return new MilvusAnnTableScanRule(this);
        }
    }

}
