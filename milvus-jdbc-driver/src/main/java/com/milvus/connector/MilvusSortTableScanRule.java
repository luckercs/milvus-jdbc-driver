package com.milvus.connector;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;

@Value.Enclosing
public class MilvusSortTableScanRule extends RelRule<MilvusSortTableScanRule.Config> {
    protected MilvusSortTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        System.out.println("hit MilvusSortTableScanRule");
        LogicalSort sort = (LogicalSort) relOptRuleCall.rels[0];
        MilvusTableScan milvusTableScan = (MilvusTableScan) relOptRuleCall.rels[1];

        boolean flag = true;
        if (sort.fetch != null) {
            if (sort.fetch.isA(SqlKind.LITERAL)) {
                milvusTableScan.getPushDownParam().setLimit(Long.parseLong(((RexLiteral) (sort.fetch)).getValue2().toString()));
            } else {
                flag = false;
            }
        }
        if (sort.offset != null) {
            if (sort.offset.isA(SqlKind.LITERAL)) {
                milvusTableScan.getPushDownParam().setOffset(Long.parseLong(((RexLiteral) (sort.offset)).getValue2().toString()));
            } else {
                flag = false;
            }
        }
        if (flag) {
            relOptRuleCall.transformTo(milvusTableScan);
        }
    }


    /**
     * LogicalSort
     *   MilvusTableScan
     *
     * ==>
     *
     * MilvusTableScan
     *
     * */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT = ImmutableMilvusSortTableScanRule.Config.builder().build()
                .withOperandSupplier(sort -> sort.operand(LogicalSort.class)
                        .inputs(scan -> scan.operand(MilvusTableScan.class).noInputs()));

        @Override
        default MilvusSortTableScanRule toRule() {
            return new MilvusSortTableScanRule(this);
        }
    }

}
