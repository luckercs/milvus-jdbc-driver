package com.milvus.connector;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;


@Value.Enclosing
public class MilvusProjectTableScanRule extends RelRule<MilvusProjectTableScanRule.Config> {
    protected MilvusProjectTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        System.out.println("hit ann rule");
        LogicalSort sort = (LogicalSort) relOptRuleCall.rels[0];
        LogicalProject project = (LogicalProject) relOptRuleCall.rel(1);
        MilvusTableScan milvusTableScan = (MilvusTableScan) relOptRuleCall.rels[2];

        // 下推limit和offset参数
        if (sort.fetch != null && sort.fetch.isA(SqlKind.LITERAL)) {
            milvusTableScan.getPushDownParam().setLimit(Long.parseLong(((RexLiteral) (sort.fetch)).getValue2().toString()));
        }
        if (sort.offset != null && sort.offset.isA(SqlKind.LITERAL)) {
            milvusTableScan.getPushDownParam().setOffset(Long.parseLong(((RexLiteral) (sort.offset)).getValue2().toString()));
        }

        // ann 参数下推到 milvusTableScan
        RexNode annExpr = null;
        int annIndex = -1;
        for (int i = 0; i < project.getProjects().size(); i++) {
            RexNode exp = project.getProjects().get(i);
            if (exp instanceof RexCall) {
                String name = ((RexCall) exp).op.getName();
                if (name.toLowerCase().equals("ann")) {
                    annExpr = exp;
                    annIndex = i;
                    List<RexNode> funcOperands = ((RexCall) exp).getOperands();
                    String vecColName = ((RexLiteral) (funcOperands.get(0))).toString().replace("\'", "");
                    String queryVec = ((RexLiteral) (funcOperands.get(1))).toString().replace("\'", "");
                    milvusTableScan.getPushDownParam().setSearchQuery(true);
                    milvusTableScan.getPushDownParam().setSearchVecColName(vecColName);
                    milvusTableScan.getPushDownParam().setSearchVec(queryVec);
                    break;
                }
            }
        }

        if (annExpr != null && annIndex != -1) {
            RelDataTypeField scoreRelDataTypeField = null;
            for (RelDataTypeField relDataTypeField : milvusTableScan.getMilvusTable().relDataType.getFieldList()) {
                if (relDataTypeField.getName().equals(MilvusTable.metaFieldScore)){
                    scoreRelDataTypeField = relDataTypeField;
                    break;
                }
            }
            if (scoreRelDataTypeField == null) {
                throw new IllegalStateException("Score field not found in table metadata");
            }
            List<RexNode> newProjects = new ArrayList<>(project.getProjects());
            RexInputRef scoreRef = new RexInputRef(scoreRelDataTypeField.getIndex(), scoreRelDataTypeField.getType());
            newProjects.set(annIndex, scoreRef);

            LogicalProject newProject = LogicalProject.create(milvusTableScan, project.getHints(), newProjects, project.getRowType().getFieldNames());
            LogicalSort newSort = LogicalSort.create(newProject, sort.getCollation(), sort.offset, sort.fetch);
            relOptRuleCall.transformTo(newSort);
        } else {
            LogicalProject newProject = project.copy(project.getTraitSet(), milvusTableScan, project.getProjects(), project.getRowType());
            LogicalSort newSort = LogicalSort.create(newProject, sort.getCollation(), sort.offset, sort.fetch);
            relOptRuleCall.transformTo(newSort);
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
                        .inputs(project -> project.operand(LogicalProject.class).predicate(p -> hasAnnFunction(p))
                                .inputs(scan -> scan.operand(MilvusTableScan.class).noInputs())));

        @Override
        default MilvusProjectTableScanRule toRule() {
            return new MilvusProjectTableScanRule(this);
        }

        static boolean hasAnnFunction(LogicalProject project) {
            for (RexNode exp : project.getProjects()) {
                if (exp instanceof RexCall) {
                    RexCall call = (RexCall) exp;
                    if (call.op.getName().equalsIgnoreCase("ann")) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

}
