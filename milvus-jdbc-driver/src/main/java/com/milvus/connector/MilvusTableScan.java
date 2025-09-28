package com.milvus.connector;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.runtime.PairList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class MilvusTableScan extends TableScan implements EnumerableRel {
    private MilvusPushDownParam milvusPushDownParam = new MilvusPushDownParam();

    protected MilvusTableScan(RelOptCluster cluster, RelOptTable table) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
    }

    public MilvusPushDownParam getPushDownParam() {
        return milvusPushDownParam;
    }

    public MilvusTranslatableTable getMilvusTable() {
        return (MilvusTranslatableTable) ((RelOptTableImpl) this.getTable()).table();
    }

    @Override
    public Result implement(EnumerableRelImplementor enumerableRelImplementor, Prefer prefer) {
        JavaTypeFactory typeFactory = enumerableRelImplementor.getTypeFactory();
        return enumerableRelImplementor.result(
                PhysTypeImpl.of(typeFactory, getRowType(), prefer.preferArray()),
                Blocks.toBlock(Expressions.call(
                        requireNonNull(table.getExpression(MilvusTranslatableTable.class)),
                        "query", enumerableRelImplementor.getRootExpression(),
                        Expressions.new_(MilvusPushDownParam.class,
                                Expressions.constant(milvusPushDownParam.getFilterExpr()),
                                Expressions.constant(milvusPushDownParam.getPartitionNames()),
                                Expressions.constant(milvusPushDownParam.getLimit()),
                                Expressions.constant(milvusPushDownParam.getOffset()),
                                Expressions.constant(milvusPushDownParam.getOutputFields()),
                                Expressions.constant(milvusPushDownParam.isSearchQuery()),
                                Expressions.constant(milvusPushDownParam.getSearchVecColName()),
                                Expressions.constant(milvusPushDownParam.getSearchVec())
                        )
                )));
    }

    @Override
    public void register(RelOptPlanner planner) {

        planner.addRule(MilvusRules.SORT_RULE);
        planner.addRule(MilvusRules.LIMIT_RULE);
//        planner.addRule(MilvusRules.LIMIT_CALC_RULE);  //无需修复，正常
//        planner.addRule(MilvusRules.PROJECT_RULE);  // 去除掉元字段
        planner.addRule(MilvusRules.FILTER_RULE);
        planner.addRule(MilvusRules.ANN_RULE);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("milvusPushDownParam", milvusPushDownParam.toString());
    }
}
