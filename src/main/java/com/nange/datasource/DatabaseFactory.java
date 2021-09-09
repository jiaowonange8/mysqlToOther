package com.nange.datasource;

import java.sql.Connection;
import java.sql.DriverManager;

import javax.sql.DataSource;

import org.slf4j.helpers.MessageFormatter;

import com.nange.constant.DatabaseProperties;
import com.nange.constant.DatabaseType;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DatabaseFactory {
	
	private static String formatUrl(DatabaseType type,String url,String port,String database) {
		Object[] argArray = { url, port, database};
		String result = MessageFormatter.arrayFormat(type.getUrl(), argArray).getMessage();
		return result;
	}

	public  static DataSource getDataSource(DatabaseProperties properties) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(formatUrl(properties.getType(), properties.getUrl(), properties.getPort(), properties.getDatabase()));
        config.setUsername(properties.getUsername());
        config.setPassword(properties.getPassword());
        config.setAutoCommit(false);
        config.setDriverClassName(properties.getType().getDriver());
		return new HikariDataSource(config);
	}
	

	public static Connection getTestConnection(DatabaseProperties properties) throws Exception {
		String url = formatUrl(properties.getType(), properties.getUrl(), properties.getPort(), properties.getDatabase());
		String username = properties.getUsername();
		String password = properties.getPassword();
		Connection con = null;
		try {
			Class.forName(properties.getType().getDriver());
			con = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			throw e;
		}
		return con;
	}
}
