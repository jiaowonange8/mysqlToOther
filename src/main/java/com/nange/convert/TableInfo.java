package com.nange.convert;

import java.util.List;

public class TableInfo {
    private String tableName;
    private List<ColumnInfo> columnList;
    private List<IndexInfo> indexList;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnInfo> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<ColumnInfo> columnList) {
        this.columnList = columnList;
    }

    public List<IndexInfo> getIndexList() {
        return indexList;
    }

    public void setIndexList(List<IndexInfo> indexList) {
        this.indexList = indexList;
    }
}
