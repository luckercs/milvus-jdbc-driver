package com.milvus.connector;

import com.milvus.functions.Ann;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;


@Value.Enclosing
public class MilvusAnnTableScanRule extends RelRule<MilvusAnnTableScanRule.Config> {
    protected MilvusAnnTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        System.out.println("hit ann rule");
        LogicalProject project = (LogicalProject) relOptRuleCall.rels[0];
        MilvusTableScan milvusTableScan = (MilvusTableScan) relOptRuleCall.rels[1];

        RexNode annExpr = null;
        int annIndex = -1;
        for (int i = 0; i < project.getProjects().size(); i++) {
            RexNode exp = project.getProjects().get(i);
            if (exp instanceof RexCall) {
                String name = ((RexCall) exp).op.getName();
                if (name.toLowerCase().equals(Ann.funcName)) {
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
                if (relDataTypeField.getName().equals(MilvusTable.metaFieldScore)) {
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
            relOptRuleCall.transformTo(newProject);
            System.out.println("hit ann and update");
        }
    }

    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT = ImmutableMilvusAnnTableScanRule.Config.builder().build()
                .withOperandSupplier(project -> project.operand(LogicalProject.class).predicate(p -> hasAnnFunction(p))
                        .inputs(scan -> scan.operand(MilvusTableScan.class).noInputs()));

        @Override
        default MilvusAnnTableScanRule toRule() {
            return new MilvusAnnTableScanRule(this);
        }

        static boolean hasAnnFunction(LogicalProject project) {
            for (RexNode exp : project.getProjects()) {
                if (exp instanceof RexCall) {
                    RexCall call = (RexCall) exp;
                    if (call.op.getName().equalsIgnoreCase(Ann.funcName)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
