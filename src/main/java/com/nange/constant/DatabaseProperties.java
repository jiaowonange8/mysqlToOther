package com.nange.constant;

public class DatabaseProperties {
	
	private DatabaseType type;
	private String url;
	private String port;
	private String database;
	private String username;
	private String password;
	
	
	
	
	public DatabaseType getType() {
		return type;
	}




	public void setType(DatabaseType type) {
		this.type = type;
	}




	public String getUrl() {
		return url;
	}




	public void setUrl(String url) {
		this.url = url;
	}




	public String getPort() {
		return port;
	}




	public void setPort(String port) {
		this.port = port;
	}




	public String getDatabase() {
		return database;
	}




	public void setDatabase(String database) {
		this.database = database;
	}




	public String getUsername() {
		return username;
	}




	public void setUsername(String username) {
		this.username = username;
	}




	public String getPassword() {
		return password;
	}




	public void setPassword(String password) {
		this.password = password;
	}




	public static  DatabaseProperties mysqlProperties() {
		DatabaseProperties p = new DatabaseProperties();
		p.setType(DatabaseType.MYSQL);
		return p;
	}
	
	public static  DatabaseProperties openGaussProperties() {
		DatabaseProperties p = new DatabaseProperties();
		p.setType(DatabaseType.OPENGAUSS);
		return p;
	}
	
	public DatabaseProperties buildUrl(String url) {
		this.setUrl(url);
		return this;
	}
	
	public DatabaseProperties buildPort(String port) {
		this.setPort(port);
		return this;
	}
	
	public DatabaseProperties buildDatabase(String database) {
		this.setDatabase(database);
		return this;
	}
	
	public DatabaseProperties buildUsername(String username) {
		this.setUsername(username);
		return this;
	}
	
	public DatabaseProperties buildPassword(String password) {
		this.setPassword(password);
		return this;
	}
}
