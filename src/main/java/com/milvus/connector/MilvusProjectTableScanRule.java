package com.milvus.connector;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;

import java.util.List;

@Value.Enclosing
public class MilvusProjectTableScanRule extends RelRule<MilvusProjectTableScanRule.Config> {
    protected MilvusProjectTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        LogicalSort sort = (LogicalSort) relOptRuleCall.rels[0];
        LogicalProject project = (LogicalProject) relOptRuleCall.rel(1);
        MilvusTableScan milvusTableScan = (MilvusTableScan) relOptRuleCall.rels[2];

        for (RexNode exp : project.getProjects()) {
            if (exp instanceof RexCall) {
                String name = ((RexCall) exp).op.getName();
                if (name.toLowerCase().equals("ann")) {
                    List<RexNode> funcOperands = ((RexCall) exp).getOperands();
                    String vecColName = ((RexLiteral)(funcOperands.get(0))).toString().replace("\'","");
                    String queryVec = ((RexLiteral)(funcOperands.get(1))).toString().replace("\'","");
                    milvusTableScan.getPushDownParam().setSearchQuery(true);
                    milvusTableScan.getPushDownParam().setSearchVecColName(vecColName);
                    milvusTableScan.getPushDownParam().setSearchVec(queryVec);
                }
            }
        }

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
            relOptRuleCall.transformTo(project);
        }

        System.out.println("hello ann");
    }


    /**
     * LogicalProject
     * MilvusTableScan
     * <p>
     * ==>
     * <p>
     * MilvusTableScan
     *
     *
     */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT = ImmutableMilvusProjectTableScanRule.Config.builder().build()
                .withOperandSupplier(sort -> sort.operand(LogicalSort.class)
                        .inputs(project -> project.operand(LogicalProject.class)
                                .inputs(scan -> scan.operand(MilvusTableScan.class).anyInputs())));

        @Override
        default MilvusProjectTableScanRule toRule() {
            return new MilvusProjectTableScanRule(this);
        }
    }

}
