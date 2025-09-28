package com.milvus.connector;

import com.milvus.functions.Ann;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
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
        System.out.println("hit MilvusProjectTableScanRule");
        LogicalProject project = (LogicalProject) relOptRuleCall.rels[0];
        MilvusTableScan milvusTableScan = (MilvusTableScan) relOptRuleCall.rels[1];

        if (milvusTableScan.getPushDownParam().isSearchQuery()) {
            return;
        }
        List<RexNode> originalProjects = project.getProjects();
        List<RexNode> newProjects = new ArrayList<>();
        List<String> newFieldNames = new ArrayList<>();
        for (int i = 0; i < originalProjects.size(); i++) {
            String fieldName = project.getRowType().getFieldNames().get(i);
            if (fieldName.equals(MilvusTable.metaFieldPartition) || fieldName.equals(MilvusTable.metaFieldScore)) {
                continue;
            }
            newProjects.add(originalProjects.get(i));
            newFieldNames.add(fieldName);
        }

        LogicalProject newProject = LogicalProject.create(milvusTableScan, project.getHints(), newProjects, newFieldNames);
        relOptRuleCall.transformTo(newProject);
        System.out.println("hit MilvusProjectTableScanRule and update");
    }

    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT = ImmutableMilvusProjectTableScanRule.Config.builder().build()
                .withOperandSupplier(project -> project.operand(LogicalProject.class).predicate(rel -> !hasAnnFunction((rel)))
                        .inputs(scan -> scan.operand(MilvusTableScan.class).noInputs()));


        @Override
        default MilvusProjectTableScanRule toRule() {
            return new MilvusProjectTableScanRule(this);
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
