package com.nange.convert.struct.service;

import com.nange.convert.ColumnInfo;
import com.nange.convert.IndexInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基础类，提供默认实现
 * 如果各数据库语法通用，在此实现即可
 */
public class SqlTransferServiceImpl extends SqlTransferService {
    private static Logger logger = LoggerFactory.getLogger(SqlTransferServiceImpl.class);

    public String dealCreateSql(String sql){
        logger.info("处理处理创建表结构语句：基类方法");
        return sql;
    }

    public String addColumnSql(ColumnInfo columnInfo){
        logger.info("开始处理添加表字段语句：基类方法");
        return "";
    }
    public String modifyColumnSql(ColumnInfo columnInfo){
        logger.info("开始处理修改表字段类型语句：基类方法");
        return "";
    }

    public String addIndexSql(IndexInfo indexInfo){
        logger.info("开始处理添加表索引语句：基类方法");
        return "";
    }

    //处理插入语句
    public String dealInsertSql(String sql){
        logger.info("开始处理插入语句：基类方法");
        return sql.replaceAll("`","\"");
    }

    //处理修改表记录语句
    public String dealUpdateSql(String sql){
        logger.info("开始处理修改表记录语句：基类方法");
        return sql.replaceAll("`","\"");
    }

    //处理删除表记录语句
    public String dealDeleteSql(String sql){
        logger.info("开始处理删除表记录语句：基类方法");
        return sql.replaceAll("`","\"");
    }

    public String reNameColumnSql(ColumnInfo columnInfo){
        logger.info("开始处理修改字段名语句：基类方法");
        String tableName = columnInfo.getTableName();
        String oldColumnName = columnInfo.getName();
        String newColumnName = columnInfo.getNewName();

        StringBuffer modifyColumnSql = new StringBuffer("ALTER TABLE \"").append(tableName.toUpperCase()).append("\"");
        modifyColumnSql.append(" RENAME COLUMN \"").append(oldColumnName.toUpperCase()).append("\"");
        modifyColumnSql.append(" TO \"").append(newColumnName.toUpperCase()).append("\"");
        modifyColumnSql.append(";\r\n");
        return modifyColumnSql.toString();
    }

    public String reNameTableSql(String oldTable, String newTable) {
        logger.info("开始处理修改表名语句：基类方法");
        return "ALTER TABLE " + oldTable + " RENAME TO "+ newTable +";\r\n";
    }


}
