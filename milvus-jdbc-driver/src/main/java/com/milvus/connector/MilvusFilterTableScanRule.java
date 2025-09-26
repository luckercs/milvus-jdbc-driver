package com.milvus.connector;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Value.Enclosing
public class MilvusFilterTableScanRule extends RelRule<MilvusFilterTableScanRule.Config> {
    protected MilvusFilterTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall relOptRuleCall) {
        System.out.println("hit MilvusFilterTableScanRule");
        LogicalFilter filter = (LogicalFilter) relOptRuleCall.rels[0];
        MilvusTableScan milvusTableScan = (MilvusTableScan) relOptRuleCall.rel(1);

        relOptRuleCall.getPlanner().getRules();

        ArrayList<String> partitions = new ArrayList<>();
        boolean withPartition = parserMilvusPartitions(filter.getCondition(), partitions, milvusTableScan.getMilvusTable());
        if (withPartition) {
            milvusTableScan.getPushDownParam().setPartitionNames(partitions);
        }

        StringBuilder exprBuilder = new StringBuilder();
        boolean parseMilvusExprSuccess = parseMilvusExpr(filter.getCondition(), exprBuilder, milvusTableScan.getMilvusTable());
        if (parseMilvusExprSuccess) {
            milvusTableScan.getPushDownParam().setFilterExpr(exprBuilder.toString());
            RexLiteral rexLiteral = milvusTableScan.getCluster().getRexBuilder().makeLiteral(true);
            LogicalFilter logicalFilter = LogicalFilter.create(milvusTableScan, rexLiteral);
            relOptRuleCall.transformTo(logicalFilter);
            System.out.println("hit MilvusFilterTableScanRule and update");
        }
    }


    private static boolean parserMilvusPartitions(@NotNull RexNode filter, List<String> partitions, MilvusTable milvusTable) {
        if (filter.isA(SqlKind.AND)) {
            for (RexNode subFilter : ((RexCall) filter).getOperands()) {
                boolean res = parserMilvusPartitions(subFilter, partitions, milvusTable);
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

    private static boolean parseMilvusExpr(RexNode filterCondition, StringBuilder filterExprBuilder, MilvusTable milvusTable) {
        RexSqlStandardConvertletTable convertletTable = new RexSqlStandardConvertletTable();
        RexToSqlNodeConverterImpl rexToSqlNodeConverter = new RexToSqlNodeConverterImpl(convertletTable) {
            @Override
            public @Nullable SqlNode convertInputRef(RexInputRef ref) {
                int index = ((RexInputRef) ref).getIndex();
                String fieldName = milvusTable.relDataType.getFieldNames().get(index);
                return new SqlIdentifier(fieldName, SqlParserPos.ZERO);
            }

            @Override
            public @Nullable SqlNode convertCall(RexCall call) {
                if (call.getOperator() == SqlStdOperatorTable.CAST) {
                    RexNode rexNode = call.getOperands().get(0);
                    if (rexNode instanceof RexInputRef) {
                        return convertInputRef((RexInputRef) rexNode);
                    } else if (rexNode instanceof RexLiteral) {
                        return convertLiteral((RexLiteral) rexNode);
                    } else {
                        throw new RuntimeException("Unsupported cast operand: " + rexNode);
                    }
                } else if (call.getOperator() == SqlStdOperatorTable.AND || call.getOperator() == SqlStdOperatorTable.OR) {
                    List<SqlNode> allOperands = new LinkedList<>();
                    collectAllOperands(call, allOperands);

                    if (allOperands.size() < 1) {
                        throw new RuntimeException("No operands for operator: " + call.getOperator());
                    }
                    SqlNode result = allOperands.get(0);
                    for (int i = 1; i < allOperands.size(); i++) {
                        SqlNode current = allOperands.get(i);
                        SqlNode newResult = call.getOperator().createCall(
                                SqlParserPos.ZERO,
                                result,
                                current
                        );
                        result = newResult;
                    }
                    return result;
                } else if (isComparisonOperator(call.getOperator())) {
                    List<SqlNode> operands = new ArrayList<>();
                    for (RexNode rexOperand : call.getOperands()) {
                        SqlNode sqlOperand = convertNode(rexOperand);
                        if (sqlOperand == null) {
                            throw new RuntimeException("Failed to convert comparison operand: " + rexOperand);
                        }
                        operands.add(sqlOperand);
                    }
                    return call.getOperator().createCall(SqlParserPos.ZERO, operands);
                } else {
                    throw new RuntimeException("Unsupported operator: " + call.getOperator());
                }
            }

            private SqlNode chainConditions(List<SqlNode> operands, SqlOperator operator) {
                SqlNode result = operands.get(0);
                for (int i = 1; i < operands.size(); i++) {
                    result = operator.createCall(
                            SqlParserPos.ZERO,
                            result,
                            operands.get(i)
                    );
                }
                return result;
            }

            private boolean isComparisonOperator(SqlOperator operator) {
                return operator == SqlStdOperatorTable.EQUALS ||
                        operator == SqlStdOperatorTable.NOT_EQUALS ||
                        operator == SqlStdOperatorTable.GREATER_THAN ||
                        operator == SqlStdOperatorTable.GREATER_THAN_OR_EQUAL ||
                        operator == SqlStdOperatorTable.LESS_THAN ||
                        operator == SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            }

            private void collectAllOperands(RexNode node, List<SqlNode> result) {

                String nodeType = node.getClass().getSimpleName();

                if (node instanceof RexCall) {
                    RexCall call = (RexCall) node;
                }

                if (node instanceof RexCall) {
                    RexCall call = (RexCall) node;
                    if (call.getOperator() == SqlStdOperatorTable.AND ||
                            call.getOperator() == SqlStdOperatorTable.OR) {
                        for (RexNode operand : call.getOperands()) {
                            collectAllOperands(operand, result);
                        }
                    } else {
                        SqlNode sqlNode = convertNode(node);
                        if (sqlNode != null) {
                            result.add(sqlNode);
                        } else {
                            throw new RuntimeException("Failed to convert node: " + node);
                        }
                    }
                } else {
                    SqlNode sqlNode = convertNode(node);
                    if (sqlNode != null) {
                        result.add(sqlNode);
                    } else {
                        throw new RuntimeException("Failed to convert node: " + node);
                    }
                }
            }

            @Override
            public @Nullable SqlNode convertLiteral(RexLiteral literal) {
                SqlNode sqlNode = super.convertLiteral(literal);
                if (sqlNode instanceof SqlLiteral && literal.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
                    return SqlLiteral.createCharString((String) literal.getValue2(), SqlParserPos.ZERO);
                }
                return sqlNode;
            }
        };

        try {
            SqlNode sqlNode = rexToSqlNodeConverter.convertNode(filterCondition);
            SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter();
            sqlNode.unparse(sqlPrettyWriter, 0, 0);
            String sqlFilter = sqlPrettyWriter.toString();

            // partition filter
            sqlFilter = sqlFilter.replaceAll("(AND |OR |^)\"" + MilvusTable.metaFieldPartition + "\" = \\S+", "").replaceAll("^\\s+(AND |OR )", "");
            sqlFilter = sqlFilter.replace("\"", "");
            sqlFilter = sqlFilter.replace(" = ", " == ");

            filterExprBuilder.append(sqlFilter);
            return true;
        } catch (Exception e) {
            return false;
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
}
