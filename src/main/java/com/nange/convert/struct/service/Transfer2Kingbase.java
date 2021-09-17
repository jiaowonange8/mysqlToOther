package com.nange.convert.struct.service;


import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nange.convert.ColumnInfo;
import com.nange.convert.IndexInfo;
import com.nange.convert.TableInfo;

/**
 *  mysql语句转kingbase语句处理类
 */
public class Transfer2Kingbase extends SqlTransferServiceImpl  {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public String dealCreateSql(String sql){
        TableInfo tableInfo = this.getTableInfo(sql);
        String tableName = tableInfo.getTableName();
        logger.info("开始处理kingbase建表语句：{}",tableName);
        List<ColumnInfo> columnList = tableInfo.getColumnList();
        List<IndexInfo> indexList = tableInfo.getIndexList();
        //id是否自增
        boolean idAutoIncrement = false;
        StringBuffer createTableSql = new StringBuffer("CREATE TABLE \"").append(tableName.toUpperCase()).append("\"(\r\n");
        for (ColumnInfo columnInfo:columnList){
            String columnName = columnInfo.getName();
            String columnType = columnInfo.getType();
            String columnLength = columnInfo.getLength();
            boolean notNull = columnInfo.isNotNull();
            String defaultValue = columnInfo.getDefaultValue();
            createTableSql.append("\"").append(columnName.toUpperCase()).append("\"");

            createTableSql.append(this.dataType2Kingbase(columnType.toLowerCase(), columnLength, defaultValue));

            if (notNull && !"id".equals(columnName.toLowerCase())) {
                createTableSql.append(" NOT NULL ");
            }
            if("id".equals(columnName.toLowerCase())){
                createTableSql.append(" PRIMARY KEY ");
                idAutoIncrement = columnInfo.isAutoIncrement();
            }
            createTableSql.append(",\r\n");
        }
        createTableSql.deleteCharAt(createTableSql.length() -3);
        createTableSql.append(");\r\n");

        for (IndexInfo indexInfo:indexList){
            String indexName = indexInfo.getName();
            String indexTable = indexInfo.getTableName();
            String indexType = indexInfo.getType();
            String[] columnNames = indexInfo.getColumnNames();
            if(indexName.length()>30){
                indexName = indexName.substring(0,29);
            }
            if("UNIQUE".equals(indexType)){
                createTableSql.append("CREATE UNIQUE INDEX ");
            } else {
                createTableSql.append("CREATE INDEX ");
            }
            createTableSql.append(indexName).append(" on \"").append(indexTable).append("\"(");
            for (String columnName:columnNames){
                createTableSql.append("\"").append(columnName.toUpperCase()).append("\",");
            }
            createTableSql.deleteCharAt(createTableSql.length() -1);
            createTableSql.append(");\r\n");
        }
        if(idAutoIncrement){
            createTableSql.append("CREATE SEQUENCE ").append(tableName).append("_SEQ START WITH 1 INCREMENT BY 1").append(";\r\n");
            createTableSql.append("CREATE OR REPLACE TRIGGER ").append(tableName).append("_TRG BEFORE INSERT ON ")
                    .append(tableName).append(" FOR EACH ROW BEGIN SELECT ")
                    .append(tableName).append("_SEQ.NEXTVAL INTO:NEW.ID FROM DUAL; END; ").append(";\r\n");
        }
        return createTableSql.toString();
    }
    @Override
    public String addColumnSql(ColumnInfo columnInfo){
        String tableName = columnInfo.getTableName();
        String columnName = columnInfo.getName();
        logger.info("开始处理Kingbase添加表字段语句:{}.{}",tableName,columnName);
        String columnType = columnInfo.getType();
        String columnLength = columnInfo.getLength();
        boolean notNull = columnInfo.isNotNull();
        String defaultValue = columnInfo.getDefaultValue();
        StringBuffer addColumnSql = new StringBuffer("ALTER TABLE \"").append(tableName.toUpperCase()).append("\"");
        addColumnSql.append(" ADD COLUMN \"").append(columnName.toUpperCase()).append("\"");

        addColumnSql.append(this.dataType2Kingbase(columnType.toLowerCase(), columnLength, defaultValue));

        if(notNull){
            addColumnSql.append(" NOT NULL ");
        }
        addColumnSql.append(";\r\n");
        return addColumnSql.toString();
    }
    @Override
    public String modifyColumnSql(ColumnInfo columnInfo){
        String tableName = columnInfo.getTableName();
        String columnName = columnInfo.getName();
        logger.info("开始处理Kingbase修改表字段类型语句:{}.{}",tableName,columnName);
        String columnType = columnInfo.getType();
        String columnLength = columnInfo.getLength();
        String defaultValue = columnInfo.getDefaultValue();

        StringBuffer modifyColumnSql = new StringBuffer("ALTER TABLE \"").append(tableName.toUpperCase()).append("\"");
        modifyColumnSql.append(" MODIFY \"").append(columnName.toUpperCase()).append("\"");

        modifyColumnSql.append(this.dataType2Kingbase(columnType.toLowerCase(), columnLength, defaultValue));

        modifyColumnSql.append(";\r\n");
        return modifyColumnSql.toString();
    }

    @Override
    public String addIndexSql(IndexInfo indexInfo){
        String indexName = indexInfo.getName();
        logger.info("开始处理Kingbase添加索引语句:{}",indexName);
        if(indexName.length()>30){
            indexName = indexName.substring(0,29);
        }
        String indexType = indexInfo.getType();
        String indexTable = indexInfo.getTableName();
        String[] columnNames = indexInfo.getColumnNames();
        StringBuffer addIndexSql = new StringBuffer();
        if("UNIQUE".equals(indexType)){
            addIndexSql.append("CREATE UNIQUE INDEX ");
        } else {
            addIndexSql.append("CREATE INDEX ");
        }
        addIndexSql.append("\"").append(indexName).append("\" ON \"").append(indexTable).append("\"(");
        for (String columnName:columnNames){
            addIndexSql.append("\"").append(columnName.toUpperCase()).append("\",");
        }
        addIndexSql.deleteCharAt(addIndexSql.length() -1);
        addIndexSql.append(");\r\n");
        return addIndexSql.toString();
    }

    //处理插入语句
    @Override
    public String dealInsertSql(String sql){
        logger.info("开始处理插入语句：Kingbase");
        sql = sql.replaceAll("`","\"");
        sql = sql.replaceAll("NOW\\(\\)","CURRENT_DATE");
        int valuesIndex = sql.indexOf("VALUES");
        if(valuesIndex >= 0){
            String sql1 = sql.substring(0,valuesIndex).toUpperCase();
            String sql2 = sql.substring(valuesIndex);
            sql = sql1 + sql2;
        }
        return sql;
    }

    //处理修改表记录语句
    @Override
    public String dealUpdateSql(String sql){
        logger.info("开始处理修改表记录语句：Kingbase");
        return sql.replaceAll("`","\"");
    }

    //处理删除表记录语句
    @Override
    public String dealDeleteSql(String sql){
        logger.info("开始处理删除表记录语句：Kingbase");
        return sql.replaceAll("`","\"");
    }


    private String dataType2Kingbase(String columnType, String columnLength, String defaultValue) {
        StringBuffer columnTypeStrb = new StringBuffer();
        switch (columnType) {
            case "tinyint":
                if (columnLength.equals("1")) {
                    columnTypeStrb.append(" BOOLEAN");
                    if (StringUtils.isNotBlank(defaultValue) && !defaultValue.toLowerCase().equals("null")) {
                        if (defaultValue.equals("1")) {
                            columnTypeStrb.append(" DEFAULT ").append("true").append(" ");
                        } else {
                            columnTypeStrb.append(" DEFAULT ").append("false").append(" ");
                        }
                    }
                }else {
                    columnTypeStrb.append(" NUMBER");
                    if (StringUtils.isNotBlank(defaultValue) && !defaultValue.toLowerCase().equals("null")) {
                        columnTypeStrb.append(" DEFAULT ").append(defaultValue).append(" ");
                    }
                }
                break;
            case "bigint":
            case "int":
                columnTypeStrb.append(" NUMBER");
                if(StringUtils.isNotBlank(defaultValue) && !defaultValue.toLowerCase().equals("null")){
                    columnTypeStrb.append(" DEFAULT ").append(defaultValue).append(" ");
                }
                break;
            case "varchar":
                columnTypeStrb.append(" NVARCHAR2").append("(").append(columnLength).append(")");
                if(StringUtils.isNotBlank(defaultValue) && !defaultValue.toLowerCase().equals("null")){
                    columnTypeStrb.append(" DEFAULT '").append(defaultValue).append("' ");
                }
                break;
            case "datetime":
                columnTypeStrb.append(" DATE");
                if(StringUtils.isNotBlank(defaultValue) && defaultValue.toLowerCase().indexOf("now") != -1){
                    columnTypeStrb.append(" DEFAULT CURRENT_DATE ");
                }
                break;
            case "text":
            case "longtext":
            case "mediumtext":
                columnTypeStrb.append(" TEXT");
                break;
            case "double":
            case "decimal":
                columnTypeStrb.append(" NUMBER(19,5)");
                break;
            case "blob":
                columnTypeStrb.append(" BLOB");
                break;
            default:
                logger.error("暂不支持该数据类型：{}", columnType);
        }
        return columnTypeStrb.toString();
    }



}
