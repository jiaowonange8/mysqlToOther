package com.nange.constant;

public enum DatabaseType {
	
	MYSQL("jdbc:mysql://{}:{}/{}?serverTimezone=GMT%2B8&useUnicode=true&useSSL=false&characterEncoding=utf-8&allowPublicKeyRetrieval=true","com.mysql.cj.jdbc.Driver"),
	OPENGAUSS("jdbc:postgresql://{}:{}/{}","org.postgresql.Driver"),
	KINGBASEES("jdbc:kingbase8://{}:{}/{}","com.kingbase8.Driver"),
	ORACLE("jdbc:oracle:thin:@//{}:{}/{}", "oracle.jdbc.OracleDriver"),
	SQLSERVER("jdbc:sqlserver://{}:{};DatabaseName={}","com.microsoft.sqlserver.jdbc.SQLServerDriver"),
	DM("jdbc:dm://{}:{}","dm.jdbc.driver.DmDriver"),
	OSCAR("jdbc:oscar://{}:{}/{}","com.oscar.Driver");
	
	private String url;
	private String driver;

	private DatabaseType(String url,String driver) {
		this.url = url;
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	

}
