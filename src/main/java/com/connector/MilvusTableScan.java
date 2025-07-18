package com.connector;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MilvusTableScan extends TableScan implements EnumerableRel {
    private static final Logger LOG = LoggerFactory.getLogger(MilvusTableScan.class);
    MilvusTable milvusTable;
    private int[] fields;

    private static List<String> searchMetaFieldNames = new ArrayList<>();

    static {
        searchMetaFieldNames.add("__search_vector_field");
        searchMetaFieldNames.add("__search_topk");
        searchMetaFieldNames.add("__search_params");
        searchMetaFieldNames.add("__search_is_vector_binary");
        searchMetaFieldNames.add("__search_vector");
        searchMetaFieldNames.add("__search_partitions");
        searchMetaFieldNames.add("__search_metric_type");
        searchMetaFieldNames.add("__search_expr");
    }


    protected MilvusTableScan(RelOptCluster cluster, List<RelHint> hints, RelOptTable relOptTable, MilvusTable milvusTable, int[] fields) {
        super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), hints, relOptTable);
        this.milvusTable = milvusTable;
        this.fields = fields;
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        LOG.info("come here");

        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());


        return implementor.result(physType, Blocks.toBlock(Expressions.call(
                table.getExpression(MilvusTable.class),
                "project",
                implementor.getRootExpression(),
                Expressions.constant(fields))));
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MilvusTableScan(getCluster(), getHints(), table, milvusTable, fields);
    }

    @Override
    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.Builder builder =
                getCluster().getTypeFactory().builder();
        for (int field : fields) {
            builder.add(fieldList.get(field));
        }
        return builder.build();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("fields", Primitive.asList(fields));
    }

    @Override
    public void register(RelOptPlanner planner) {
        LOG.info("come here");
        planner.addRule(MilvusRules.FILTER_SCAN);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        LOG.info("come here");
        return super.computeSelfCost(planner, mq)
                .multiplyBy(((double) fields.length + 2D)
                        / ((double) table.getRowType().getFieldCount() + 2D));
    }


}
