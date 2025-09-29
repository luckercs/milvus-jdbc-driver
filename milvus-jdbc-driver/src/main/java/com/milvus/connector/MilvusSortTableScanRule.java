package com.milvus.connector;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
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
        Sort sort = null;
        MilvusTableScan milvusTableScan = null;
        for (RelNode rel : relOptRuleCall.rels) {
            if (rel instanceof Sort) {
                sort = (Sort) rel;
            }
            if (rel instanceof MilvusTableScan) {
                milvusTableScan = (MilvusTableScan) rel;
            }
        }
        if (sort == null || milvusTableScan == null) {
            throw new RuntimeException("sort or milvusTableScan is null");
        }

        // 下推limit和offset参数
        if (sort.fetch != null && sort.fetch.isA(SqlKind.LITERAL)) {
            milvusTableScan.getPushDownParam().setLimit(Long.parseLong(((RexLiteral) (sort.fetch)).getValue2().toString()));
        }
        if (sort.offset != null && sort.offset.isA(SqlKind.LITERAL)) {
            milvusTableScan.getPushDownParam().setOffset(Long.parseLong(((RexLiteral) (sort.offset)).getValue2().toString()));
        }
    }

    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT = ImmutableMilvusSortTableScanRule.Config.builder().build()
                .withOperandSupplier(sort -> sort.operand(Sort.class)
                        .inputs(scan -> scan.operand(RelNode.class)
                                .predicate(rel -> hasMilvusTableScanAncestor(rel)).anyInputs()));


        @Override
        default MilvusSortTableScanRule toRule() {
            return new MilvusSortTableScanRule(this);
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
