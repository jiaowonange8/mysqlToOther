package com.nange.coretransfer;

import com.nange.convert.utils.FileUtils;
import com.nange.datasource.DatabaseExeHandler;

import javax.swing.*;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class TransferMainHandlerOracle extends AbstractTransferMainHandler{
    @Override
    public void targetTableDataTransfer(DatabaseExeHandler sourceHandler, DatabaseExeHandler targetHandler, JTextArea textArea, Set<String> keySet
                ,Map<String, List<String>> idAutoIncrementTable) throws SQLException {
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
                tableInsertPreSql.append("insert into ").append(key).append(" values (");
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
                            targetPreparedStatement.setObject(i, pageResultSet.getObject(i));
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
}
