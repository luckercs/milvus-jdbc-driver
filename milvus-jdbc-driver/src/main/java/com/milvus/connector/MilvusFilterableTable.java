package com.milvus.connector;

import com.milvus.util.MilvusProxy;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class MilvusFilterableTable extends MilvusTable implements FilterableTable {

    public MilvusFilterableTable(MilvusProxy milvusProxy, String collectionName) {
        super(milvusProxy, collectionName);
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext dataContext, List<RexNode> filters) {
        StringBuilder filterExprBuilder = new StringBuilder();
        filters.removeIf(filter -> addFilter(filter, filterExprBuilder, collectionDesc));
        MilvusPushDownParam milvusPushDownParam = new MilvusPushDownParam();
        milvusPushDownParam.setFilterExpr(filterExprBuilder.toString());
        return new AbstractEnumerable<@Nullable Object[]>() {
            @Override
            public Enumerator<@Nullable Object[]> enumerator() {
                return new MilvusEnumerator<>(MilvusFilterableTable.this, milvusPushDownParam);
            }
        };
    }

    private static boolean addFilter(RexNode filter, StringBuilder filterExprBuilder, DescribeCollectionResp collectionDesc) {
        if (filter.isA(SqlKind.AND)) {
            for (RexNode subFilter : ((RexCall) filter).getOperands()) {
                boolean res = addFilter(subFilter, filterExprBuilder, collectionDesc);
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
                String fieldName = collectionDesc.getCollectionSchema().getFieldSchemaList().get(index).getName();
                String value = ((RexLiteral) right).getValue2().toString();
                if (!filterExprBuilder.toString().equals("")) {
                    filterExprBuilder.append(" AND ");
                }
                filterExprBuilder.append(fieldName);
                filterExprBuilder.append("==");
                filterExprBuilder.append(value);
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
                String fieldName = collectionDesc.getCollectionSchema().getFieldSchemaList().get(index).getName();
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
                String fieldName = collectionDesc.getCollectionSchema().getFieldSchemaList().get(index).getName();
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
                String fieldName = collectionDesc.getCollectionSchema().getFieldSchemaList().get(index).getName();
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
                String fieldName = collectionDesc.getCollectionSchema().getFieldSchemaList().get(index).getName();
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
                String fieldName = collectionDesc.getCollectionSchema().getFieldSchemaList().get(index).getName();
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
}
