package com.vmware.dim.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.dim.jdbc.output.ConfigurationParser;
import com.vmware.dim.jdbc.output.JDBCConnection;
import com.vmware.dim.jdbc.output.OperationType;
import com.vmware.dim.jdbc.output.PreparedStatement;
import com.vmware.dim.jdbc.output.PreparedStatementBean;
import com.vmware.dim.jdbc.output.PreparedStatementCache;
import com.vmware.dim.jdbc.output.Regex;
import com.vmware.dim.jdbc.output.Script;
import com.vmware.dim.output.OutputStreamWriter;

/**
 * JDBC output target class implementing {@link OutputStreamWriter}<br>
 * Reads the data from Redis and pushes to JDBC
 * 
 * @author ghimanshu
 *
 */
public class JDBCOutputStream extends OutputStreamWriter {

	private static final Logger logger = LogManager.getLogger(JDBCOutputStream.class);

	private JDBCConnection connection;
	private ConfigurationParser configurationParser;
	private PreparedStatementCache preparedStatementCache;
	private boolean batchCommit = false;

	public JDBCOutputStream(ConfigurationDetailsDTO configurationDetailsDTO) throws Exception {
		super(configurationDetailsDTO);
		logger.traceEntry();
		/** Initialize JDBC Connection */
		initConnection();

		/** Initialize Configuration */
		configurationParser = new ConfigurationParser();
		configurationParser.init(configurationDetailsDTO);
		
		batchCommit = configuration.get("batchCommit") == null ? false : Boolean.valueOf(configuration.get("batchCommit"));
		logger.debug("Batch Commit set to "+ batchCommit);
		/** Initialize Prepared Statement Cache */
		preparedStatementCache = new PreparedStatementCache();

		logger.traceExit();
	}

	private void initConnection() throws Exception{
		connection = new JDBCConnection();
		connection.init(configuration.get("URL"), configuration.get("UserName"), configuration.get("Password"),
				configuration.get("Driver"));
	}
	/**
	 * Method to write data to JDBC for each Redis key and value
	 */
	@Override
	public void write(Map<String, String> redisData) {
		
		Connection conn = connection.get();
		
		logger.traceEntry();
		try {
			if(conn.isClosed()){
				logger.debug("Connection closed. Reestablishing connection");
				initConnection();
			}
			
			for (String name : redisData.keySet()) {
				logger.debug("name = " + name);
				String value = redisData.get(name);
				logger.debug("value = " + value);

				DocumentContext documentContext = JsonPath.parse(value);

				for (String key : configurationParser.get().keySet()) {

					// Get the type of event i.e. insert/update/delete/upsert
					logger.debug("Query Key "+ key);
					String expr = Regex.replace(configurationParser.get().get(key).get(key + ".type"), documentContext);
					logger.debug("Operation Type "+ expr);
					OperationType operationTypes = OperationType.valueOf(Script.eval(expr).toString());

					PreparedStatementBean preparedStatementBean = null;
					String multiplexField = configurationParser.get().get(key).get(key+".multiplex_field");
					/** Special Case to handle upsert */
					if (operationTypes == OperationType.upsert) {
						logger.debug("Operation type "+ operationTypes);
						/**
						 * Execute the select (to check for upsert) prepared
						 * statement
						 */
						preparedStatementBean = getPreparedStatement(key, "upsert");
						
						com.vmware.dim.jdbc.output.PreparedStatement.bindValues(preparedStatementBean, documentContext,
								configurationParser.get().get(key).get(key + "." + operationTypes.name() + "_mapping"), connection.get(),multiplexField);
						
						ResultSet resultSet = preparedStatementBean.preparedStatement.executeQuery();
						resultSet.next();

						logger.debug("Upsert select query's count "+ resultSet.getInt(1));
						/** Update */
						if (resultSet.getInt(1) > 0) {
							operationTypes = OperationType.update;
						} /** Insert */
						else {
							operationTypes = OperationType.insert;
						}
					}

					logger.debug("Final operation type detected "+ operationTypes);

					preparedStatementBean = getPreparedStatement(key, operationTypes.name());
					/** Bind the values in prepared statement */
					PreparedStatement.bindValues(preparedStatementBean, documentContext,
							configurationParser.get().get(key).get(key + "." + operationTypes.name() + "_mapping"), connection.get(), multiplexField);

					logger.debug("Executing "+ key);
				}
				if(!batchCommit){
					commit();	
				}
				logger.debug("Transaction Commit - batchCommit : "+ batchCommit);
			}
			if(batchCommit){
				commit();
			}
		} catch (Exception e) {
			logger.error("Error inserting data to JDBC", e);
			try {
				if(e instanceof SQLException){
					SQLException sqlException = (SQLException)e;
					logger.error("JDBC Exception ", sqlException.getNextException());
				}
				rollback();
			} catch (Exception e1) {
				logger.error("Error rolling back the connection", e);
			}
		}
		logger.traceExit();
	}
	
	private void commit() throws Exception{
		for(PreparedStatementBean bean : preparedStatementCache.getValues()){
			logger.debug("Execute Batch");
			bean.preparedStatement.executeBatch();
			bean.preparedStatement.clearBatch();
		}
		logger.debug("Commit");
		connection.get().commit();
	}
	
	private void rollback() throws SQLException{
		for(PreparedStatementBean bean : preparedStatementCache.getValues()){
			logger.debug("Clearing Batch");
			bean.preparedStatement.clearBatch();
		}
		logger.debug("Rollback");
		connection.get().rollback();;
	}

	private PreparedStatementBean getPreparedStatement(String key, String operationType) throws SQLException {

		String cacheKey = key + "." + operationType;
		PreparedStatementBean preparedStatementBean = preparedStatementCache.get(cacheKey);
		if (preparedStatementBean == null) {
			String query = configurationParser.get().get(key).get(key + "." + operationType);
			preparedStatementBean = com.vmware.dim.jdbc.output.PreparedStatement.create(query, connection.get());
			preparedStatementCache.add(cacheKey, preparedStatementBean);
		}
		return preparedStatementBean;
	}
}
