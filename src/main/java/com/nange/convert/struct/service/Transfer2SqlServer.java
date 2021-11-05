package com.nange.convert.struct.service;

import com.nange.convert.ColumnInfo;
import com.nange.convert.IndexInfo;
import com.nange.convert.TableInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * @author SIMBA1949
 * @date 2021/10/20 10:27
 */
public class Transfer2SqlServer extends SqlTransferService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 转换建表语句
     * @param sql（建表语句）
     * @return string
     */
    @Override
    public String dealCreateSql(String sql) {
        TableInfo tableInfo = this.getTableInfo(sql);
        String tableName = tableInfo.getTableName();
        logger.info("开始处理SqlServer建表语句：{}",tableName);
        List<ColumnInfo> columnList = tableInfo.getColumnList();
        List<IndexInfo> indexList = tableInfo.getIndexList();

        StringBuilder createTableSql = getCreateTableSql(tableName, columnList);

        createTableSql.deleteCharAt(createTableSql.length() - 3);
        createTableSql.append(");\r\n");
        for (IndexInfo indexInfo : indexList) {
            String indexName = indexInfo.getName();
            String indexTable = indexInfo.getTableName();
            String indexType = indexInfo.getType();
            String[] columnNames = indexInfo.getColumnNames();
            if("UNIQUE".equals(indexType)){
                createTableSql.append("CREATE UNIQUE INDEX ");
            } else {
                createTableSql.append("CREATE INDEX ");
            }
            createTableSql.append(indexName)
                    .append(" on [").append(indexTable).append("](");
            for (String columnName : columnNames) {
                createTableSql.append("[").append(columnName).append("],");
            }
            createTableSql.deleteCharAt(createTableSql.length() - 1);
            createTableSql.append(");\r\n");
        }
        return createTableSql.toString();
    }

    private StringBuilder getCreateTableSql(String tableName, List<ColumnInfo> columnList) {
        StringBuilder createTableSql = new StringBuilder("CREATE TABLE [").append(tableName.toUpperCase()).append("](\r\n");
        for (ColumnInfo columnInfo : columnList) {
            String columnName = columnInfo.getName();
            String columnType = columnInfo.getType();
            String columnLength = columnInfo.getLength();
            boolean notNull = columnInfo.isNotNull();
            String defaultValue = columnInfo.getDefaultValue();
            createTableSql.append("[").append(columnName.toLowerCase()).append("]");
            createTableSql.append(" ");
            String dataType = dataType2SqlServer(columnType.toLowerCase(), columnLength);
            createTableSql.append(dataType);
            if (notNull && !"id".equalsIgnoreCase(columnName)) {
                createTableSql.append(" NOT NULL ");
            }
            getDefaultValue(createTableSql, defaultValue);

            if ("id".equalsIgnoreCase(columnName)) {
                if(columnInfo.isAutoIncrement()){
                    createTableSql.append(" identity(1,1) PRIMARY KEY ");
                } else {
                    createTableSql.append(" PRIMARY KEY ");
                }
            }
            createTableSql.append(",\r\n");
        }
        return createTableSql;
    }

    private void getDefaultValue(StringBuilder createTableSql, String defaultValue) {
        if (StringUtils.isNotBlank(defaultValue) && !"null".equalsIgnoreCase(defaultValue)) {
            if (defaultValue.toLowerCase().contains("now")) {
                createTableSql.append(" DEFAULT sysdatetime() ");
            } else {
                createTableSql.append(" DEFAULT ('").append(defaultValue).append("') ");
            }
        }
    }

    @Override
    public String addColumnSql(ColumnInfo columnInfo) {
        String tableName = columnInfo.getTableName();
        String columnName = columnInfo.getName();
        logger.info("开始处理SqlServer添加表字段语句:{}.{}",tableName,columnName);
        String columnType = columnInfo.getType();
        String columnLength = columnInfo.getLength();
        boolean notNull = columnInfo.isNotNull();
        String defaultValue = columnInfo.getDefaultValue();
        StringBuffer addColumnSql = new StringBuffer("ALTER TABLE [").append(tableName.toUpperCase()).append("]");
        addColumnSql.append(" ADD [").append(columnName).append("]");
        addColumnSql.append(" ");
        String dataType = dataType2SqlServer(columnType.toLowerCase(), columnLength);
        addColumnSql.append(dataType);
        if (notNull) {
            addColumnSql.append(" NOT NULL ");
        }
        if (StringUtils.isNotBlank(defaultValue) && !defaultValue.equalsIgnoreCase("null")) {
            if (defaultValue.toLowerCase().contains("now")) {
                addColumnSql.append(" DEFAULT sysdatetime() ");
            } else {
                addColumnSql.append(" DEFAULT ('").append(defaultValue).append("') ");
            }
        }
        addColumnSql.append(";\r\n");
        return addColumnSql.toString();
    }

    @Override
    public String modifyColumnSql(ColumnInfo columnInfo) {
        String tableName = columnInfo.getTableName();
        String columnName = columnInfo.getName();
        logger.info("开始处理SqlServer修改表字段语句:{}.{}",tableName,columnName);
        String columnType = columnInfo.getType();
        String columnLength = columnInfo.getLength();
        StringBuffer modifyColumnSql = new StringBuffer("ALTER TABLE [").append(tableName.toUpperCase()).append("]");
        modifyColumnSql.append(" ALTER COLUMN [").append(columnName).append("]");
        modifyColumnSql.append(" ");
        String dataType = dataType2SqlServer(columnType.toLowerCase(), columnLength);
        modifyColumnSql.append(dataType);
        modifyColumnSql.append(";\r\n");
        return modifyColumnSql.toString();
    }
    //处理修改表名
    @Override
    public String reNameTableSql(String oldTable, String newTable) {
        logger.info("开始处理 SqlServer 修改表名称语句:{} to {}",oldTable, newTable);

        StringBuffer modifyColumnSql = new StringBuffer("EXEC sp_rename ");
        modifyColumnSql.append(" '").append(oldTable.toUpperCase()).append("','").append(newTable.toUpperCase()).append("'");
        modifyColumnSql.append(";\r\n");
        return modifyColumnSql.toString();
    }

    //处理修改字段名
    @Override
    public String reNameColumnSql(ColumnInfo columnInfo){
        String tableName = columnInfo.getTableName();
        String oldColumnName = columnInfo.getName();
        String newColumnName = columnInfo.getNewName();
        logger.info("开始处理 SqlServer 修改表字段名称语句:{}.{}.{}",tableName, oldColumnName, newColumnName);

        StringBuffer modifyColumnSql = new StringBuffer("EXEC sp_rename ");
        modifyColumnSql.append(" '[").append(tableName.toUpperCase()).append("].[").append(oldColumnName.toUpperCase()).append("]'");
        modifyColumnSql.append(" ,'").append(newColumnName.toUpperCase()).append("'");
        modifyColumnSql.append(";\r\n");
        return modifyColumnSql.toString();
    }

    @Override
    public String addIndexSql(IndexInfo indexInfo) {
        String indexName = indexInfo.getName();
        logger.info("开始处理SqlServer添加索引语句:{}",indexName);
        if (indexName.length() > 30) {
            String indexLeft = indexName.substring(0, 15);
            Random rand = new Random();
            indexName=indexLeft.concat(String.valueOf(rand.nextInt(1000)));
        }
        String indexTable = indexInfo.getTableName();
        String indexType = indexInfo.getType();
        String[] columnNames = indexInfo.getColumnNames();
        StringBuffer addIndexSql = new StringBuffer();
        if("UNIQUE".equals(indexType)){
            addIndexSql.append("CREATE UNIQUE INDEX ");
        } else {
            addIndexSql.append("CREATE INDEX ");
        }
        addIndexSql.append("[").append(indexName).append("] ON [").append(indexTable).append("](");
        for (String columnName : columnNames) {
            addIndexSql.append("[").append(columnName).append("],");
        }
        addIndexSql.deleteCharAt(addIndexSql.length() - 1);
        addIndexSql.append(");\r\n");
        return addIndexSql.toString();
    }

    private String dataType2SqlServer(String dataType, String length) {
        StringBuffer buffer = new StringBuffer();
        switch (dataType) {
            case "bigint":
            case "tinyint":
            case "datetime":
            case "int":
                buffer.append(dataType);
                break;
            case "varchar":
                buffer.append("nvarchar(").append(length).append(")");
                break;
            case "varbinary":
                buffer.append("varbinary(").append(length).append(")");
                break;
            case "text":
            case "longtext":
            case "mediumtext":
                buffer.append("nvarchar(MAX)");
                break;
            case "double":
            case "decimal":
                buffer.append("decimal(19,5)");
                break;
            case "blob":
                buffer.append("varbinary(MAX)");
                break;
            default:
                logger.error("暂不支持该数据类型：{}", dataType);
        }
        return buffer.toString();
    }

    /**
     * 处理插入语句
     * @param sql（插入语句）
     * @return string
     */
    @Override
    public String dealInsertSql(String sql){
        logger.info("开始处理插入语句：SqlServer");
        return sql.replaceAll("`","").replaceAll("now", "SYSDATETIME").replaceAll("NOW", "SYSDATETIME");
    }

    /**
     * 处理修改表记录语句
     * @param sql（修改表记录语句）
     * @return string
     */
    @Override
    public String dealUpdateSql(String sql){
        logger.info("开始处理修改表记录语句：SqlServer");
        return sql.replaceAll("`","").replaceAll("now", "SYSDATETIME").replaceAll("NOW", "SYSDATETIME");
    }

    /**
     * 处理删除表记录语句
     * @param sql（删除表记录语句）
     * @return string
     */
    @Override
    public String dealDeleteSql(String sql){
        logger.info("开始处理删除表记录语句：SqlServer");
        return sql.replaceAll("`","");
    }

}
