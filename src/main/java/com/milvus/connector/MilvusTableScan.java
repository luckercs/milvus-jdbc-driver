package com.milvus.connector;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;

import static java.util.Objects.requireNonNull;

public class MilvusTableScan extends TableScan implements EnumerableRel {
    protected MilvusTableScan(RelOptCluster cluster, RelOptTable table) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
    }

    @Override
    public Result implement(EnumerableRelImplementor enumerableRelImplementor, Prefer prefer) {
        return enumerableRelImplementor.result(
                PhysTypeImpl.of(enumerableRelImplementor.getTypeFactory(), getRowType(), prefer.preferArray()),
                Blocks.toBlock(Expressions.call(
                        requireNonNull(table.getExpression(MilvusTranslatableTable.class)),
                        "query", enumerableRelImplementor.getRootExpression()
                )));

    }
}
