package connector;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MilvusFilterTableScanRule extends RelRule<MilvusFilterTableScanRule.Config> {
    private static final Logger LOG = LoggerFactory.getLogger(MilvusFilterTableScanRule.class);

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    protected MilvusFilterTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LOG.info("come here");
        // 解析搜索参数
        LogicalFilter logicalFilter = call.rel(0);
        RexNode condition = logicalFilter.getCondition();

        // 解析表参数
        MilvusTableScan milvusTableScan = call.rel(1);
        List<String> fieldNames = milvusTableScan.milvusTable.getFieldNames();

        // 解析搜索参数
        Map<String, String> searchParams = new HashMap<>();
        if (condition.isA(SqlKind.AND)) {
            List<RexNode> operands = ((RexCall) condition).getOperands();
            for (RexNode rexNode : operands) {
                if (rexNode.isA(SqlKind.EQUALS)) {
                    List<RexNode> operands1 = ((RexCall) rexNode).getOperands();
                    RexInputRef rexInputRef = (RexInputRef) (operands1.get(0));
                    int index1 = rexInputRef.getIndex();
                    RexLiteral rexLiteral = (RexLiteral) (operands1.get(1));
                    String value = rexLiteral.getValue2().toString();
                    if (MilvusTable.metaFieldNames.contains(fieldNames.get(index1))) {
                        searchParams.put(fieldNames.get(index1), value);
                    }
                }
            }
        }

        if (searchParams.size() > 0) {

            milvusTableScan.milvusTable.setSearchParams(searchParams);
            milvusTableScan.milvusTable.removeMetaField();

            int[] fields = identityList(milvusTableScan.milvusTable.getFieldNames().size());

            RelOptTable relOptTable = milvusTableScan.getTable();

            RelOptTableImpl relOptTable1 = RelOptTableImpl.create(relOptTable.getRelOptSchema(), milvusTableScan.milvusTable.getRowType(), milvusTableScan.milvusTable, ImmutableList.copyOf(relOptTable.getQualifiedName()));

            MilvusTableScan milvusTableScanSearch = new MilvusTableScan(milvusTableScan.getCluster(), milvusTableScan.getHints(), relOptTable1, milvusTableScan.milvusTable, fields);

            // 构建常量condition
//            SqlBinaryOperator sqlBinaryOperator = new SqlBinaryOperator();


            RexBuilder rexBuilder = logicalFilter.getInput().getCluster().getRexBuilder();
            RexLiteral rexLiteral = rexBuilder.makeLiteral(true);
            call.rels[0] = LogicalFilter.create(milvusTableScanSearch, rexLiteral);
            call.rels[1] = milvusTableScanSearch;

            call.transformTo(milvusTableScanSearch);
            LOG.info("Go milvus search");
        }
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableMilvusFilterTableScanRule.Config.of()
                .withOperandSupplier(b2 -> b2.operand(LogicalFilter.class)
                        .oneInput(b3 -> b3.operand(MilvusTableScan.class).noInputs()));


        @Override
        default MilvusFilterTableScanRule toRule() {
            return new MilvusFilterTableScanRule(this);
        }
    }

    private static int[] getProjectFields(List<RexNode> exps) {
        final int[] fields = new int[exps.size()];
        for (int i = 0; i < exps.size(); i++) {
            final RexNode exp = exps.get(i);
            if (exp instanceof RexInputRef) {
                fields[i] = ((RexInputRef) exp).getIndex();
            } else {
                return null; // not a simple projection
            }
        }
        return fields;
    }

    private int[] identityList(int n) {
        int[] integers = new int[n];
        for (int i = 0; i < n; i++) {
            integers[i] = i;
        }
        return integers;
    }
}
