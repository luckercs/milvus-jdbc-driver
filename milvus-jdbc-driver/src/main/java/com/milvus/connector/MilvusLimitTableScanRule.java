package com.milvus.connector;

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;

@Value.Enclosing
public class MilvusLimitTableScanRule extends RelRule<MilvusLimitTableScanRule.Config> {
    protected MilvusLimitTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        System.out.println("hit MilvusLimitTableScanRule");
        EnumerableLimit limit = null;
        MilvusTableScan milvusTableScan = null;
        for (RelNode rel : relOptRuleCall.rels) {
            if (rel instanceof EnumerableLimit) {
                limit = (EnumerableLimit) rel;
            }
            if (rel instanceof MilvusTableScan) {
                milvusTableScan = (MilvusTableScan) rel;
            }
        }
        if (limit == null || milvusTableScan == null) {
            throw new RuntimeException("limit or milvusTableScan is null");
        }

        if (limit.fetch != null && limit.fetch.isA(SqlKind.LITERAL)) {
            milvusTableScan.getPushDownParam().setLimit(Long.parseLong(((RexLiteral) (limit.fetch)).getValue2().toString()));
        }
        if (limit.offset != null && limit.offset.isA(SqlKind.LITERAL)) {
            milvusTableScan.getPushDownParam().setOffset(Long.parseLong(((RexLiteral) (limit.offset)).getValue2().toString()));
        }
    }

    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT = ImmutableMilvusLimitTableScanRule.Config.builder().build()
                .withOperandSupplier(sort -> sort.operand(EnumerableLimit.class)
                        .inputs(scan -> scan.operand(RelNode.class)
                                .predicate(rel -> hasMilvusTableScanAncestor(rel)).anyInputs()));


        @Override
        default MilvusLimitTableScanRule toRule() {
            return new MilvusLimitTableScanRule(this);
        }

        static boolean hasMilvusTableScanAncestor(RelNode rel) {
            if (rel == null) {
                return false;
            }
            if (rel instanceof MilvusTableScan) {
                return true;
            }
            for (RelNode input : rel.getInputs()) {
                if (hasMilvusTableScanAncestor(input)) {
                    return true;
                }
            }
            return false;
        }
    }
}
