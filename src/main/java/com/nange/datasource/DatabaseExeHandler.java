package com.nange.datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import com.nange.constant.DatabaseProperties;

public class DatabaseExeHandler {
	
	private DataSource dataSource;
	
	public Connection getConnect() throws SQLException {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connection;
	}
	public PreparedStatement createPreparedStatement(Connection connection,String updateSql) throws SQLException {
		return connection.prepareStatement(updateSql);
	}
	
	public Statement createStatement(Connection connection) throws SQLException {
		return connection.createStatement();
	}
	
	
	
	public DatabaseExeHandler(DataSource dataSource) {
		super();
		this.dataSource =  dataSource;
	}

	public void closeProcess(Statement stmt) {
        if(stmt!=null){
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
	}
	
	public void closeProcess(PreparedStatement preStmt) {
        if(preStmt!=null){
            try {
            	preStmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
	}
	
	public void closeProcess(ResultSet rs) {
		if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
	}
	
	public void closeProcess(Connection connection) {
        if(connection!=null){
            try {
            	connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
	}


	

}
