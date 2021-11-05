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
 * @author sunchaofei
 */
public class Transfer2Oscar extends SqlTransferServiceImpl {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * 转换建表语句
	 * @param sql（建表语句）
	 * @return string
	 */
	@Override
	public String dealCreateSql(String sql) {
		TableInfo tableInfo = this.getTableInfo(sql);
		String tableName = tableInfo.getTableName();
		logger.info("开始处理 神通 建表语句：{}", tableName);
		List<ColumnInfo> columnList = tableInfo.getColumnList();
		List<IndexInfo> indexList = tableInfo.getIndexList();
		StringBuffer createTableSql = new StringBuffer("CREATE TABLE ").append(tableName.toUpperCase()).append("(\r\n");
		for (ColumnInfo columnInfo : columnList) {
			String columnName = columnInfo.getName();
			String columnType = columnInfo.getType();
			String columnLength = columnInfo.getLength();
			boolean notNull = columnInfo.isNotNull();
			String defaultValue = columnInfo.getDefaultValue();
			createTableSql.append(columnName.toLowerCase());
			createTableSql.append(" ");
			String dataType = dataType2Oscar(columnType.toLowerCase(), columnLength);
			createTableSql.append(dataType);
			if (notNull && !"id".equalsIgnoreCase(columnName)) {
				createTableSql.append(" NOT NULL ");
			}
			if (StringUtils.isNotBlank(defaultValue) && !"null".equalsIgnoreCase(defaultValue)) {
				if (defaultValue.toLowerCase().contains("now") || defaultValue.toUpperCase().contains("CURRENT_TIMESTAMP")) {
					createTableSql.append(" DEFAULT NOW() ");
				} else {
					createTableSql.append(" DEFAULT '").append(defaultValue).append("' ");
				}
			}
			if ("id".equalsIgnoreCase(columnName)) {
				if (columnInfo.isAutoIncrement()) {
					createTableSql.append(" AUTO_INCREMENT PRIMARY KEY ");
				} else {
					createTableSql.append(" PRIMARY KEY ");
				}
			}
			createTableSql.append(",\r\n");
		}
		createTableSql.deleteCharAt(createTableSql.length() - 3);
		createTableSql.append(");\r\n");
		for (IndexInfo indexInfo : indexList) {
			String indexName = indexInfo.getName();
			String indexTable = indexInfo.getTableName();
			String indexType = indexInfo.getType();
			String[] columnNames = indexInfo.getColumnNames();
			if ("UNIQUE".equals(indexType)) {
				createTableSql.append("CREATE UNIQUE INDEX ");
			} else {
				createTableSql.append("CREATE INDEX ");
			}
			createTableSql.append(indexName)
					.append(" on ").append(indexTable).append("(");
			for (String columnName : columnNames) {
				createTableSql.append(columnName).append(",");
			}
			createTableSql.deleteCharAt(createTableSql.length() - 1);
			createTableSql.append(");\r\n");
		}
		return createTableSql.toString();
	}

	@Override
	public String addColumnSql(ColumnInfo columnInfo) {
		String tableName = columnInfo.getTableName();
		String columnName = columnInfo.getName();
		logger.info("开始处理 神通 添加表字段语句:{}.{}", tableName, columnName);

		String columnType = columnInfo.getType();
		String columnLength = columnInfo.getLength();
		boolean notNull = columnInfo.isNotNull();
		String defaultValue = columnInfo.getDefaultValue();
		StringBuffer addColumnSql = new StringBuffer("ALTER TABLE ").append(tableName.toUpperCase());
		addColumnSql.append(" ADD ").append(columnName);
		addColumnSql.append(" ");
		String dataType = dataType2Oscar(columnType.toLowerCase(), columnLength);
		addColumnSql.append(dataType);
		if (notNull) {
			addColumnSql.append(" NOT NULL ");
		}
		if (StringUtils.isNotBlank(defaultValue) && !defaultValue.equalsIgnoreCase("null")) {
			if (defaultValue.toLowerCase().contains("now")) {
				addColumnSql.append(" DEFAULT NOW() ");
			} else {
				addColumnSql.append(" DEFAULT '").append(defaultValue).append("' ");
			}
		}
		addColumnSql.append(";\r\n");
		return addColumnSql.toString();
	}

	@Override
	public String modifyColumnSql(ColumnInfo columnInfo) {
		String tableName = columnInfo.getTableName();
		String columnName = columnInfo.getName();
		logger.info("开始处理 神通 修改表字段语句:{}.{}", tableName, columnName);
		String columnType = columnInfo.getType();
		String columnLength = columnInfo.getLength();
		StringBuffer modifyColumnSql = new StringBuffer("ALTER TABLE ").append(tableName.toUpperCase());
		modifyColumnSql.append(" MODIFY ").append(columnName);
		modifyColumnSql.append(" ");
		String dataType = dataType2Oscar(columnType.toLowerCase(), columnLength);
		modifyColumnSql.append(dataType);
		modifyColumnSql.append(";\r\n");
		return modifyColumnSql.toString();
	}

	@Override
	public String addIndexSql(IndexInfo indexInfo) {
		String indexName = indexInfo.getName();
		logger.info("开始处理 神通 添加索引语句:{}", indexName);
		if (indexName.length() > 30) {
			String indexLeft = indexName.substring(0, 15);
			Random rand = new Random();
			indexName=indexLeft.concat(String.valueOf(rand.nextInt(1000)));
		}
		String indexTable = indexInfo.getTableName();
		String indexType = indexInfo.getType();
		String[] columnNames = indexInfo.getColumnNames();
		StringBuffer addIndexSql = new StringBuffer();
		if ("UNIQUE".equals(indexType)) {
			addIndexSql.append("CREATE UNIQUE INDEX ");
		} else {
			addIndexSql.append("CREATE INDEX ");
		}
		addIndexSql.append(indexName).append(" ON ").append(indexTable).append("(");
		for (String columnName : columnNames) {
			addIndexSql.append("").append(columnName).append(",");
		}
		addIndexSql.deleteCharAt(addIndexSql.length() - 1);
		addIndexSql.append(");\r\n");
		return addIndexSql.toString();
	}

	private String dataType2Oscar(String dataType, String length) {
		StringBuffer buffer = new StringBuffer();
		switch (dataType) {
			case "double":
			case "decimal":
				buffer.append("decimal(19,5)");
				break;
			case "varchar":
			case "blob":
				buffer.append(dataType);
				if (StringUtils.isNotBlank(length)) {
					length = length.contains(",") ? length : String.valueOf(Integer.valueOf(length) * 3);
					buffer.append("(").append(length).append(")");
				}
				break;
			case "varbinary":
				buffer.append("varbinary(").append(length).append(")");
				break;
			case "bigint":
			case "tinyint":
			case "int":
				buffer.append(dataType);
				break;
			case "text":
			case "longtext":
			case "mediumtext":
				buffer.append("text");
				break;
			case "datetime":
				buffer.append("timestamp");
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
	public String dealInsertSql(String sql) {
		logger.info("开始处理插入语句 ：神通 ");
		return sql.replaceAll("`", "");
	}

	/**
	 * 处理修改表记录语句
	 * @param sql（修改表记录语句）
	 * @return string
	 */
	@Override
	public String dealUpdateSql(String sql) {
		logger.info("开始处理修改表记录语句 ：神通 ");
		return sql.replaceAll("`", "");
	}

	/**
	 * 处理删除表记录语句
	 * @param sql（删除表记录语句）
	 * @return string
	 */
	@Override
	public String dealDeleteSql(String sql) {
		logger.info("开始处理删除表记录语句 ：神通 ");
		return sql.replaceAll("`", "");
	}
}
