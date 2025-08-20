package com.milvus.connector;

import java.util.List;

public class MilvusPushDownParam{
    private String filterExpr;
    private List<String> partitionNames;
    private Long limit;
    private Long offset;
    private List<String> outputFields;

    public MilvusPushDownParam() {
    }

    public MilvusPushDownParam(String filterExpr, List<String> partitionNames, Long limit, Long offset, List<String> outputFields) {
        this.filterExpr = filterExpr;
        this.partitionNames = partitionNames;
        this.limit = limit;
        this.offset = offset;
        this.outputFields = outputFields;
    }

    public String getFilterExpr() {
        return filterExpr;
    }

    public void setFilterExpr(String filterExpr) {
        this.filterExpr = filterExpr;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    public Long getLimit() {
        return limit;
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public List<String> getOutputFields() {
        return outputFields;
    }

    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }
}
