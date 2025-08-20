package com.milvus.connector;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

@Value.Enclosing
public class MilvusFilterTableScanRule extends RelRule<MilvusFilterTableScanRule.Config> {
    protected MilvusFilterTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        LogicalFilter filter = (LogicalFilter) relOptRuleCall.rels[0];
        MilvusTableScan milvusTableScan = (MilvusTableScan) relOptRuleCall.rel(1);

        StringBuilder exprBuilder = new StringBuilder();
        boolean res = addFilter(filter.getCondition(), exprBuilder, milvusTableScan.getMilvusTable());
        ArrayList<String> partitions = new ArrayList<>();
        boolean withPartition = parserPartitions(filter.getCondition(), partitions, milvusTableScan.getMilvusTable());
        if (withPartition) {
            milvusTableScan.getPushDownParam().setPartitionNames(partitions);
        }
        if (res) {
            milvusTableScan.getPushDownParam().setFilterExpr(exprBuilder.toString());
            relOptRuleCall.transformTo(milvusTableScan);
        }
    }


    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableMilvusFilterTableScanRule.Config.builder().build()
                .withOperandSupplier(filter -> filter.operand(LogicalFilter.class)
                        .inputs(scan -> scan.operand(MilvusTableScan.class).noInputs()));

        @Override
        default MilvusFilterTableScanRule toRule() {
            return new MilvusFilterTableScanRule(this);
        }
    }

    private static boolean addFilter(RexNode filter, StringBuilder filterExprBuilder, MilvusTable milvusTable) {
        if (filter.isA(SqlKind.AND)) {
            for (RexNode subFilter : ((RexCall) filter).getOperands()) {
                boolean res = addFilter(subFilter, filterExprBuilder, milvusTable);
                if (!res) {
                    return false;
                }
            }
            return true;
        } else if (filter.isA(SqlKind.EQUALS)) {
            RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                String fieldName = milvusTable.relDataType.getFieldNames().get(index);
                String value = ((RexLiteral) right).getValue2().toString();
                if (index < milvusTable.collectionDesc.getFieldNames().size()) {
                    if (!filterExprBuilder.toString().equals("")) {
                        filterExprBuilder.append(" AND ");
                    }
                    filterExprBuilder.append(fieldName);
                    filterExprBuilder.append("==");
                    filterExprBuilder.append(value);
                }
                return true;
            }
        } else if (filter.isA(SqlKind.GREATER_THAN)) {
            RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                String fieldName = milvusTable.relDataType.getFieldNames().get(index);
                String value = ((RexLiteral) right).getValue2().toString();
                if (!filterExprBuilder.toString().equals("")) {
                    filterExprBuilder.append(" AND ");
                }
                filterExprBuilder.append(fieldName);
                filterExprBuilder.append(">");
                filterExprBuilder.append(value);
                return true;
            }
        } else if (filter.isA(SqlKind.LESS_THAN)) {
            RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                String fieldName = milvusTable.relDataType.getFieldNames().get(index);
                String value = ((RexLiteral) right).getValue2().toString();
                if (!filterExprBuilder.toString().equals("")) {
                    filterExprBuilder.append(" AND ");
                }
                filterExprBuilder.append(fieldName);
                filterExprBuilder.append("<");
                filterExprBuilder.append(value);
                return true;
            }
        } else if (filter.isA(SqlKind.GREATER_THAN_OR_EQUAL)) {
            RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                String fieldName = milvusTable.relDataType.getFieldNames().get(index);
                String value = ((RexLiteral) right).getValue2().toString();
                if (!filterExprBuilder.toString().equals("")) {
                    filterExprBuilder.append(" AND ");
                }
                filterExprBuilder.append(fieldName);
                filterExprBuilder.append(">=");
                filterExprBuilder.append(value);
                return true;
            }
        } else if (filter.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
            RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                String fieldName = milvusTable.relDataType.getFieldNames().get(index);
                String value = ((RexLiteral) right).getValue2().toString();
                if (!filterExprBuilder.toString().equals("")) {
                    filterExprBuilder.append(" AND ");
                }
                filterExprBuilder.append(fieldName);
                filterExprBuilder.append("<=");
                filterExprBuilder.append(value);
                return true;
            }
        } else if (filter.isA(SqlKind.NOT_EQUALS)) {
            RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                String fieldName = milvusTable.relDataType.getFieldNames().get(index);
                String value = ((RexLiteral) right).getValue2().toString();
                if (!filterExprBuilder.toString().equals("")) {
                    filterExprBuilder.append(" AND ");
                }
                filterExprBuilder.append(fieldName);
                filterExprBuilder.append("!=");
                filterExprBuilder.append(value);
                return true;
            }
        }
        /**
         *  todo: support more milvus filter conditions； remove from filter list
         *        ==,!=,>,<,>=, <=  (done)
         *        json字段过滤
         *        数组字段过滤
         *        字符串文本匹配
         *        IN 和 LIKE
         *        + - * / % **
         *        AND OR NOT
         *        is null  is not null
         * */
        return false;
    }

    private static boolean parserPartitions(RexNode filter, List<String> partitions, MilvusTable milvusTable) {
        if (filter.isA(SqlKind.AND)) {
            for (RexNode subFilter : ((RexCall) filter).getOperands()) {
                boolean res = parserPartitions(subFilter, partitions, milvusTable);
                if (res) {
                    return true;
                }
            }
            return false;
        } else if (filter.isA(SqlKind.EQUALS)) {
            RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef && right instanceof RexLiteral) {
                int index = ((RexInputRef) left).getIndex();
                String fieldName = milvusTable.relDataType.getFieldNames().get(index);
                String value = ((RexLiteral) right).getValue2().toString();
                if (fieldName.equals(MilvusTable.metaFieldPartition)) {
                    partitions.add(value);
                    return true;
                }
            }
        }
        // todo: support partitions list
        return false;
    }
}
