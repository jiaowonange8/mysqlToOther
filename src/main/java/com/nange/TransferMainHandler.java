package com.nange;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;
import javax.swing.JTextArea;

import com.nange.constant.DatabaseProperties;
import com.nange.constant.DatabaseType;
import com.nange.convert.utils.FileUtils;
import com.nange.convert.utils.SqlDataUtils;
import com.nange.datasource.DatabaseExeHandler;
import com.nange.datasource.DatabaseFactory;

public class TransferMainHandler {
	
	public static void transfer(JTextArea textArea,DatabaseExeHandler sourceHandler,DatabaseExeHandler targetHandler,DatabaseType type) {
		FileUtils.writeText("data.log", " \r\n", false);
		writeLog(textArea, "开始转移数据 \r\n");
		try {
			//①：获取源数据库表结构
			Map<String, String> originTableStruct = originTableStruct(sourceHandler);
			writeLog(textArea, "============1：源数据库全量表结构生成完毕============"+"\r\n");
			//②：目标数据库表结构构造
			Map<String, String> targetTableStruct = targetTableStruct(originTableStruct, type);
			writeLog(textArea, "============2：目标数据库全量表结构生成完毕============"+"\r\n");
			//③ 目标数据库表结构创建
			targetTableCreate(targetHandler,targetTableStruct,textArea);
			writeLog(textArea, "============3：目标数据库表结构创建完毕============"+"\r\n");
			//④ 目标数据库表数据迁移
			targetTableDataTransfer(sourceHandler,targetHandler,textArea,targetTableStruct.keySet());
			writeLog(textArea, "============4：目标数据库表数据迁移完毕============"+"\r\n");
		} catch (SQLException e) {
			writeLog(textArea, e.getMessage()+"\r\n");
			return;
		}
		writeLog(textArea, "end: ============数据库转移完毕============"+"\r\n");

	}
	
	private static void targetTableDataTransfer(DatabaseExeHandler sourceHandler, DatabaseExeHandler targetHandler, JTextArea textArea,
			Set<String> keySet)  throws SQLException{
		//循环所有表，查出源数据库表数据，往目标数据库迁移
		for(String key : keySet) {
			Connection originConnection = null;
			Statement originStatement = null;
			Connection targetConnect = null;
			try {
				//①：拼凑insert预处理语句
				originConnection = sourceHandler.getConnect();
				originStatement = sourceHandler.createStatement(originConnection);
				ResultSet columnResultSet = originStatement.executeQuery("select * from "+key+" limit 1");
				//表字段数
				int columnCount = columnResultSet.getMetaData().getColumnCount();
				 StringBuffer tableInsertPreSql =new StringBuffer();
				 tableInsertPreSql.append("insert into "+key+" values (");
		        String link ="";
		        for (int i = 0; i <columnCount ; i++) {
		        	tableInsertPreSql.append(link).append("?");
		            link=",";
		        }
		        tableInsertPreSql.append(")");
		        //②：赋值预处理语句,分页处理
		        targetConnect = targetHandler.getConnect();
		        PreparedStatement targetPreparedStatement = targetHandler.createPreparedStatement(targetConnect,tableInsertPreSql.toString());
		        
		        //表总条数
		        long dataCount = 0;
		        //分页个数
		        int length = 10000;
		        ResultSet countResultSet = originStatement.executeQuery("select count(*) from "+key);
				while(countResultSet.next()) {
					dataCount = countResultSet.getLong(1);
				}
				if(dataCount == 0) {
					//表中无数据
					continue;
				}
				//总分页次数
				int pageCount = (int) (dataCount/length +1);
				for(int page = 0;page<pageCount;page++) {
					ResultSet pageResultSet = originStatement.executeQuery("select * from "+key+" limit "+length+" offset "+page*length);
					while(pageResultSet.next()){
						for(int i=1;i<=columnCount;i++) {
							Object object = pageResultSet.getObject(i);
							if(object instanceof byte[]) {
								targetPreparedStatement.setBlob(i, pageResultSet.getBlob(i));
							}else {
								targetPreparedStatement.setObject(i, pageResultSet.getObject(i));
							}
						}
						targetPreparedStatement.addBatch();
					}
					targetPreparedStatement.executeBatch();
					targetPreparedStatement.clearBatch();
					sourceHandler.closeProcess(pageResultSet);
				}
		        //③：关闭目标数据库结果集
				sourceHandler.closeProcess(columnResultSet);
				sourceHandler.closeProcess(countResultSet);
				sourceHandler.closeProcess(originStatement);
		        targetHandler.closeProcess(targetPreparedStatement);
				writeLog(textArea, "============目标数据库表 "+key+" 数据转移完毕============"+"\r\n");
			} 
			catch (SQLException e) {
				e.printStackTrace();
				writeLog(textArea, "============目标数据库表 "+key+" 数据转移报错============"+e.getMessage()+"\r\n");
				throw e;
			}finally {
				sourceHandler.closeProcess(originConnection);
				targetHandler.closeProcess(targetConnect);
			}
		}
		
	}

	private static void targetTableCreate(DatabaseExeHandler targetHandler, Map<String, String> targetTableStruct,JTextArea textArea) throws SQLException{
		Connection targetConnect = null;
		Statement targetStatement = null;
		try {
			targetConnect = targetHandler.getConnect();
			targetStatement = targetHandler.createStatement(targetConnect);
		} catch (SQLException e) {
			throw new SQLException("=============创建目标表结构时报错============"+e.getMessage());
		}
		for(String key : targetTableStruct.keySet()) {
			try {
				targetStatement.execute(targetTableStruct.get(key));
				writeLog(textArea, "============目标数据库表结构 "+key+" 创建完毕============"+"\r\n");
			} catch (SQLException e) {
				throw new SQLException("=============创建目标表结构  "+key+" 报错============"+e.getMessage());
			}
		}
		targetHandler.closeProcess(targetStatement);
		targetHandler.closeProcess(targetConnect);
	}

	private static Map<String, String> targetTableStruct(Map<String, String> originTableStruct, DatabaseType type) throws SQLException {
		Map<String, String> targetTableStruct = null;
		try {
			targetTableStruct = SqlDataUtils.targetTableStruct(originTableStruct, type);
		} catch (SQLException e) {
			throw e;
		}
		return targetTableStruct;
	}

	private static Map<String, String> originTableStruct(DatabaseExeHandler sourceHandler) throws SQLException{
		Map<String, String> originTableStruct = null;
		try {
			originTableStruct = SqlDataUtils.originTableStruct(sourceHandler);
		} catch (SQLException e) {
			throw e;
		}
		return originTableStruct;
	}
	
	private static void writeLog(JTextArea textArea,String log) {
		FileUtils.writeText("data.log", log, true);
		if(textArea!=null) {
			textArea.append(log);
		}
	}

}
