package com.milvus.connector;

import java.util.List;

public class MilvusPushDownParam {
    private String filterExpr;
    private List<String> partitionNames;
    private Long limit;
    private Long offset;
    private List<String> outputFields;
    private boolean searchQuery = false;
    private String searchVecColName;
    private String searchVec;
    private String searchParams;

    public MilvusPushDownParam() {
    }

    public MilvusPushDownParam(String filterExpr, List<String> partitionNames, Long limit, Long offset, List<String> outputFields, boolean searchQuery, String searchVecColName, String searchVec, String searchParams) {
        this.filterExpr = filterExpr;
        this.partitionNames = partitionNames;
        this.limit = limit;
        this.offset = offset;
        this.outputFields = outputFields;
        this.searchQuery = searchQuery;
        this.searchVecColName = searchVecColName;
        this.searchVec = searchVec;
        this.searchParams = searchParams;
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

    public boolean isSearchQuery() {
        return searchQuery;
    }

    public void setSearchQuery(boolean searchQuery) {
        this.searchQuery = searchQuery;
    }

    public String getSearchVecColName() {
        return searchVecColName;
    }

    public void setSearchVecColName(String searchVecColName) {
        this.searchVecColName = searchVecColName;
    }

    public String getSearchVec() {
        return searchVec;
    }

    public void setSearchVec(String searchVec) {
        this.searchVec = searchVec;
    }

    public String getSearchParams() {
        return searchParams;
    }

    public void setSearchParams(String searchParams) {
        this.searchParams = searchParams;
    }

    @Override
    public String toString() {
        return "MilvusPushDownParam{" +
                "filterExpr='" + filterExpr + '\'' +
                ", partitionNames=" + partitionNames +
                ", limit=" + limit +
                ", offset=" + offset +
                ", outputFields=" + outputFields +
                ", searchQuery=" + searchQuery +
                ", searchVecColName='" + searchVecColName + '\'' +
                ", searchVec='" + searchVec + '\'' +
                ", searchParams='" + searchParams + '\'' +
                '}';
    }
}
