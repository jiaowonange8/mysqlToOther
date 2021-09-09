package com.nange.convert.struct.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.nange.convert.ColumnInfo;
import com.nange.convert.IndexInfo;
import com.nange.convert.TableInfo;

/**
 * 数据结构转移类
 */
public abstract class SqlTransferService {
//	private Logger logger = LoggerFactory.getLogger(this.getClass());

	public abstract String dealCreateSql(String sql);
	public abstract String dealInsertSql(String sql);
	public abstract String dealUpdateSql(String sql);
	public abstract String dealDeleteSql(String sql);

	public abstract String addColumnSql(ColumnInfo columnInfo);
	public abstract String modifyColumnSql(ColumnInfo columnInfo);
	public abstract String addIndexSql(IndexInfo indexInfo);
	public abstract String reNameColumnSql(ColumnInfo columnInfo);
	public abstract String reNameTableSql(String oldTable, String newTable);

//	private String transferSql(String sql){
//		if(sql.endsWith(";")) {
//			sql = sql.substring(0,sql.length() -1 );
//		}
//		if(sql.startsWith("INSERT ")){
//			sql = this.dealInsertSql(sql);
//		} else if(sql.startsWith("CREATE ") && !sql.contains("CREATE INDEX ") && !sql.contains("CREATE UNIQUE ")){
//			sql = this.dealCreateSql(sql);
//		} else if(sql.startsWith("ALTER ")){
//			sql = this.dealAlterSql(sql);
//		} else if(sql.startsWith("UPDATE ")){
//			sql = this.dealUpdateSql(sql);
//		} else if(sql.startsWith("DELETE ")){
//			sql = this.dealDeleteSql(sql);
//		}
//		if(!sql.endsWith(";\r\n")){
//			if(sql.endsWith(";")){
//				sql = sql + "\r\n";
//			} else {
//				sql = sql + ";\r\n";
//			}
//		}
//		return sql;
//	}

	//基类方法方法，待提取
	public TableInfo getTableInfo(String sql){
		sql = StringUtils.replace(sql,"\r\n","\n");
		TableInfo tableInfo = new TableInfo();
		int headIndex = sql.indexOf("(");
		String healsql = sql.substring(0,headIndex);
		healsql = StringUtils.replace(healsql," ","");
		healsql = StringUtils.replace(healsql,"`","");
		String tableName = healsql.substring(11);
		tableInfo.setTableName(tableName);

		List<ColumnInfo> columnList = new ArrayList<>();
		List<IndexInfo> indexList = new ArrayList<>();

		String bodysql = sql.substring(headIndex + 1,sql.lastIndexOf(")"));
		String[] bodyArr = bodysql.split(",\n");
		for (String bodystr:bodyArr) {
			bodystr = this.trim(bodystr);
			if(!bodystr.startsWith("PRIMARY")){//主健此处理不做处理，在建表时以字段名id自动添加
				if(bodystr.indexOf("INDEX ") >= 0 || (bodystr.indexOf("KEY ") >= 0 && bodystr.indexOf("PRIMARY ") ==-1)){//处理索引列
					bodystr = StringUtils.replace(bodystr,"`","");
					String indexName = "";
					if(bodystr.indexOf("INDEX ") >= 0){
						indexName = bodystr.substring(bodystr.indexOf("INDEX") + 5,bodystr.indexOf("("));
					} else if(bodystr.indexOf("KEY ") >= 0){
						indexName = bodystr.substring(bodystr.indexOf("KEY") + 3,bodystr.indexOf("("));
					}
					String indexType = (bodystr.indexOf("UNIQUE ") >=0)? "UNIQUE":"";
					String columns = bodystr.substring(bodystr.indexOf("(") + 1,bodystr.indexOf(")"));
					columns = StringUtils.replace(columns," ","");
					String[] columnNames = columns.split(",");
					IndexInfo indexInfo = new IndexInfo();
					indexInfo.setTableName(tableName);
					indexInfo.setName(indexName);
					indexInfo.setType(indexType);
					indexInfo.setColumnNames(columnNames);

					indexList.add(indexInfo);
				} else {
					ColumnInfo columnInfo = new ColumnInfo();
					int commentIndex = bodystr.indexOf(" COMMENT");
					if(commentIndex >0){
						bodystr = bodystr.substring(0,commentIndex);
					}
					if(bodystr.indexOf("AUTO_INCREMENT") > 0){
						columnInfo.setAutoIncrement(true);
						bodystr = StringUtils.replace(bodystr,"AUTO_INCREMENT","");
					}
					if(bodystr.indexOf("NOT ") > 0){
						columnInfo.setNotNull(true);
						bodystr = StringUtils.replace(bodystr,"NOT ","");
						bodystr = StringUtils.replace(bodystr,"NULL","");
					}
					bodystr = StringUtils.replace(bodystr,"PRIMARY KEY","");
					String[] strarr = bodystr.split(" ");
					List<String> strlist = new ArrayList<>();
					for (String tstr:strarr){
						if(StringUtils.isNotBlank(tstr) && !"DEFAULT".equals(tstr.trim())){
							tstr = StringUtils.replace(tstr.trim(),"`","");
							tstr = StringUtils.replace(tstr.trim(),"'","");
							strlist.add(tstr);
						}
					}
					for (int i = 0; i < strlist.size(); i++) {
						if(i ==0){
							columnInfo.setName(strlist.get(i));
						} else if(i ==1){
							String tcolumnTypeStr = strlist.get(i);
							String columnType = "";
							if(tcolumnTypeStr.indexOf("(") == -1){
								columnType = tcolumnTypeStr.trim();
							} else {
								columnType = tcolumnTypeStr.substring(0,tcolumnTypeStr.indexOf("("));
								String columnLength = tcolumnTypeStr.substring(tcolumnTypeStr.indexOf("(") + 1,tcolumnTypeStr.indexOf(")"));
								columnInfo.setLength(columnLength);
								if("varchar".equals(columnType.toLowerCase())){
									if (StringUtils.isNotBlank(columnLength) && Integer.parseInt(columnLength) >= 4000) {
										columnType = "TEXT";
									}
								}
							}
							columnInfo.setType(columnType);
						} else if(i ==2){
							columnInfo.setDefaultValue(strlist.get(i));
						}
					}
					columnList.add(columnInfo);
				}
			}
		}
		tableInfo.setColumnList(columnList);
		tableInfo.setIndexList(indexList);
		return tableInfo;
	}

	//处理修改表结构语句
	public String dealAlterSql(String sql){
		String tmpsql = StringUtils.replace(sql," ","");
		if(tmpsql.toLowerCase().indexOf("addcolumn") > 0){
			ColumnInfo columnInfo = this.getColumnInfoByAlter(sql);
			return this.addColumnSql(columnInfo);
		} else if(tmpsql.toLowerCase().indexOf("modifycolumn") > 0){
			ColumnInfo columnInfo = this.getColumnInfoByAlter(sql);
			return this.modifyColumnSql(columnInfo);
		} else if(tmpsql.toLowerCase().indexOf("addindex") > 0){
			IndexInfo indexInfo = this.getIndexInfoByAlter(sql);
			return this.addIndexSql(indexInfo);
		} else if(tmpsql.toLowerCase().indexOf("adduniqueindex") > 0){
			IndexInfo indexInfo = this.getIndexInfoByAlter(sql);
			return this.addIndexSql(indexInfo);
		} else if(tmpsql.toLowerCase().indexOf("renameto") > 0){
			List<String> tables = this.getTable2(sql);
			return this.reNameTableSql(tables.get(0), tables.get(1));
		} else if(tmpsql.toLowerCase().indexOf("changecolumn") > 0){
			ColumnInfo columnInfo = this.getColumnInfoByRename(sql);
			return this.reNameColumnSql(columnInfo);
		}
		return sql;
	}
	//通过建索引语句组装索引信息
	public ColumnInfo getColumnInfoByRename(String alterSql){
		ColumnInfo columnInfo = new ColumnInfo();
		List<String> strlist = new ArrayList<>();
		int commentIndex = alterSql.indexOf("COMMENT");
		if(commentIndex >0){
			alterSql = alterSql.substring(0,commentIndex);
		}
		alterSql = StringUtils.replace(alterSql,"`","");
		if(alterSql.indexOf("NOT ") > 0){
			columnInfo.setNotNull(true);
			alterSql = StringUtils.replace(alterSql,"NOT ","");
			alterSql = StringUtils.replace(alterSql,"NULL","");
		}
		alterSql = this.trim(alterSql);

		String[] temparr = alterSql.split(" ");
		for(String tempstr:temparr){
			if(StringUtils.isNotBlank(tempstr) && !"DEFAULT".equals(tempstr.trim())){
				tempstr = StringUtils.replace(tempstr.trim(),"`","");
				tempstr = StringUtils.replace(tempstr.trim(),"'","");
				strlist.add(tempstr);
			}
		}
		for (int i = 0; i < strlist.size(); i++) {
			if(i==2){
				columnInfo.setTableName(strlist.get(i));
			} else if(i==5){
				columnInfo.setName(strlist.get(i));
			} else if(i==6){
				columnInfo.setNewName(strlist.get(i));
			} else if(i==7){
				String tcolumnTypeStr = strlist.get(i);
				String columnType = "";
				if(tcolumnTypeStr.indexOf("(") == -1){
					columnType = tcolumnTypeStr.trim();
				} else {
					columnType = tcolumnTypeStr.substring(0,tcolumnTypeStr.indexOf("("));
					String columnLength = tcolumnTypeStr.substring(tcolumnTypeStr.indexOf("(") + 1,tcolumnTypeStr.indexOf(")"));
					columnInfo.setLength(columnLength);
				}
				columnInfo.setType(columnType);
			} else if(i ==8){
				columnInfo.setDefaultValue(strlist.get(i));
			}
		}
		return columnInfo;
	}

	//通过建索引语句组装索引信息
	public List<String> getTable2(String alterSql){
		alterSql = StringUtils.replace(alterSql,"`","");
		alterSql = StringUtils.replace(alterSql,";","");

		List<String> strlist = new ArrayList<>();
		String[] temparr = alterSql.split(" ");
		for(String tempstr:temparr){
			if(StringUtils.isNotBlank(tempstr)){
				strlist.add(tempstr);
			}
		}
		List<String> list = new ArrayList<>();
		list.add(strlist.get(2));
		list.add(strlist.get(5));
		return list;
	}

	//通过alter语句组装字段信息
	public ColumnInfo getColumnInfoByAlter(String alterSql){
		ColumnInfo columnInfo = new ColumnInfo();
		List<String> strlist = new ArrayList<>();
		int commentIndex = alterSql.indexOf("COMMENT");
		if(commentIndex >0){
			alterSql = alterSql.substring(0,commentIndex);
		}
		alterSql = StringUtils.replace(alterSql,"`","");
		if(alterSql.indexOf("NOT ") > 0){
			columnInfo.setNotNull(true);
			alterSql = StringUtils.replace(alterSql,"NOT ","");
			alterSql = StringUtils.replace(alterSql,"NULL","");
		}
		alterSql = this.trim(alterSql);

		String[] temparr = alterSql.split(" ");
		for(String tempstr:temparr){
			if(StringUtils.isNotBlank(tempstr) && !"DEFAULT".equals(tempstr.trim())){
				tempstr = StringUtils.replace(tempstr.trim(),"`","");
				tempstr = StringUtils.replace(tempstr.trim(),"'","");
				strlist.add(tempstr);
			}
		}
		for (int i = 0; i < strlist.size(); i++) {
			if(i==2){
				columnInfo.setTableName(strlist.get(i));
			} else if(i==5){
				columnInfo.setName(strlist.get(i));
			} else if(i==6){
				String tcolumnTypeStr = strlist.get(i);
				String columnType = "";
				if(tcolumnTypeStr.indexOf("(") == -1){
					columnType = tcolumnTypeStr.trim();
				} else {
					columnType = tcolumnTypeStr.substring(0,tcolumnTypeStr.indexOf("("));
					String columnLength = tcolumnTypeStr.substring(tcolumnTypeStr.indexOf("(") + 1,tcolumnTypeStr.indexOf(")"));
					columnInfo.setLength(columnLength);
				}
				columnInfo.setType(columnType);
			} else if(i ==7){
				columnInfo.setDefaultValue(strlist.get(i));
			}
		}
		return columnInfo;
	}

	//通过建索引语句组装索引信息
	public IndexInfo getIndexInfoByAlter(String alterSql){
		IndexInfo indexInfo = new IndexInfo();
		String tmpsql = alterSql;
		tmpsql = StringUtils.replace(tmpsql,"`","");
		tmpsql = StringUtils.replace(tmpsql," ","");
		int i1 = tmpsql.indexOf("ADDINDEX");
		int i2 = tmpsql.indexOf("ADDUNIQUEINDEX");
		String tableName = "";
		if(i1 >= 0){
			tableName = tmpsql.substring(10,i1);
			alterSql = alterSql.substring(alterSql.indexOf("INDEX") + 5);
		} else if(i2 >= 0){
			tableName = tmpsql.substring(10,i2);
			alterSql = alterSql.substring(alterSql.indexOf("INDEX") + 5);
			indexInfo.setType("UNIQUE");
		}

		alterSql = StringUtils.replace(alterSql,"`","");
		alterSql = StringUtils.replace(alterSql," ","");
		String indexName = alterSql.substring(0,alterSql.indexOf("("));
		String columns = alterSql.substring(alterSql.indexOf("(") + 1,alterSql.indexOf(")"));
		String[] columnNames = columns.split(",");
		indexInfo.setTableName(tableName);
		indexInfo.setName(indexName);
		indexInfo.setColumnNames(columnNames);
		return indexInfo;
	}

	private String trim(String str){
		str = str.trim();
		str = StringUtils.replace(str," (","(");
		str = StringUtils.replace(str,"( ","(");
		str = StringUtils.replace(str," )",")");
		str = StringUtils.replace(str," ,",",");
		str = StringUtils.replace(str,", ",",");
		return str;
	}
}
