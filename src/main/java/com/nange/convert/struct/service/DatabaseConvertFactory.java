package com.nange.convert.struct.service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nange.constant.DatabaseType;


/**
 * sql转换服务工程
 */
public class DatabaseConvertFactory {
	
	protected static Logger logger = LoggerFactory.getLogger(DatabaseConvertFactory.class);
	
	public static SqlTransferService getSqlTransfer(DatabaseType dbType) {
		switch (dbType){
			case KINGBASEES:
				return new Transfer2Kingbase();
			case OPENGAUSS:
				return new Transfer2OpenGauss();
			case ORACLE:
				return new Transfer2Oracle();
			case SQLSERVER:
				return new Transfer2SqlServer();
			case DM:
				return new Transfer2DM();
			case OSCAR:
				return new Transfer2Oscar();
			default:
				logger.error("无效的数据库类型：{}", dbType);
				return null;
		}
	}
}
