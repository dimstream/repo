package com.vmware.dim.jdbc.output;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JDBCConnection {

	private static final Logger logger = LogManager.getLogger(JDBCConnection.class);
	private Connection connection;
	
	public void init(String connectionURl, String userName, String password, String driverClass) throws Exception{
		logger.traceEntry(connectionURl, userName, driverClass);
		if(this.connection == null){
			Class.forName(driverClass);
			this.connection = DriverManager.getConnection(connectionURl, userName, password);	
			this.connection.setAutoCommit(false);
		}
		logger.traceExit("Connection established");
	}
	
	public Connection get(){
		return this.connection;
	}
	
}
