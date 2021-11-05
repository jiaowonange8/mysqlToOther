package com.nange.convert.utils;

import com.nange.constant.DatabaseType;
import com.nange.convert.struct.service.DatabaseConvertFactory;
import com.nange.datasource.DatabaseExeHandler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SqlDataUtils {
	
	/**
	 * 获取源数据库表结构
	 * @param handler
	 * @return
	 * @throws SQLException 
	 */
	public static Map<String, String> originTableStruct(DatabaseExeHandler handler) throws SQLException{
		Map<String, String> tablesMap = new HashMap<String, String>();
		Statement mysqlStatement = null;
		Connection connect = null;
		ResultSet tablesReqult = null;
		try {
			connect = handler.getConnect();
			mysqlStatement = handler.createStatement(connect);
			tablesReqult = mysqlStatement.executeQuery("Show tables");
			while(tablesReqult.next()) {
				String tableName = tablesReqult.getString(1);
//				if(!"callback_record_history".equals(tableName.toLowerCase())) {
//					continue;
//				}
				Statement tableStructStatement = handler.createStatement(connect);
				ResultSet tableStructQesult = tableStructStatement.executeQuery("SHOW CREATE TABLE "+tableName);
				while(tableStructQesult.next()) {
					String tableStruct = tableStructQesult.getString(2);
					tablesMap.put(tableName, tableStruct);
				}
				handler.closeProcess(tableStructQesult);
				handler.closeProcess(tableStructStatement);
			}
		} catch (SQLException e) {
			writeLog("获取源数据库表结构异常"+e.getMessage()+" \r\n");
			throw new SQLException("获取源数据库表结构异常:"+e.getMessage());
		}finally {
			handler.closeProcess(tablesReqult);
			handler.closeProcess(mysqlStatement);
			handler.closeProcess(connect);
		}
		return tablesMap;
	}
	
	/**
	 * 目标数据库表结构转换
	 * @param originTableStruct
	 * @param type
	 * @return
	 */
	public static Map<String, String> targetTableStruct(Map<String, String> originTableStruct, DatabaseType type) throws SQLException{
		Map<String, String> targetTableStruct = new HashMap<String, String>();
		try {
			for (Map.Entry<String, String> entry : originTableStruct.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				String dealCreateSql = Objects.requireNonNull(DatabaseConvertFactory.getSqlTransfer(type)).dealCreateSql(value);
				targetTableStruct.put(key, dealCreateSql);
			}
			originTableStruct = null;
		} catch (Exception e) {
			writeLog("构造目标数据库表结构异常"+e.getMessage()+" \r\n");
			throw new SQLException("构造目标数据库表结构异常"+e.getMessage());
		}
		return targetTableStruct;
	}

	private static void writeLog(String log) {
		FileUtils.writeText("data.log", log, true);
	}
}
