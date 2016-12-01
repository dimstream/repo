package com.vmware.dim.jdbc.output;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.PathNotFoundException;
import com.vmware.dim.jdbc.datatype.mapping.DataType;
import com.vmware.dim.jdbc.datatype.mapping.DateDataTypeMapper;

import net.minidev.json.JSONArray;

public class PreparedStatement {

	private static final Logger logger = LogManager.getLogger(PreparedStatement.class);
	
	public static PreparedStatementBean create(String query, Connection connection) throws SQLException {
		logger.traceEntry(query);
		List<String> bindValues = Regex.getBindValues(query);
		/*for (String value : bindValues) {
			query = query.replaceAll("\\" + value, "?");
		}*/
		query = query.replaceAll(Regex.BIND_VALUE_REGEX_PATTERN, "?");
		logger.debug("Final query "+ query);
		java.sql.PreparedStatement preparedStatement = connection.prepareStatement(query);
		PreparedStatementBean preparedStatementBean = new PreparedStatementBean();
		preparedStatementBean.preparedStatement = preparedStatement;
		preparedStatementBean.bindValueConfs = bindValues;
		logger.debug("Bind Values "+ preparedStatement);
		return logger.traceExit(preparedStatementBean);
	}

	public static void bindValues(PreparedStatementBean preparedStatementBean, DocumentContext documentContext, String mappingConf, Connection connection, String multiplexField) throws Exception{
		logger.traceEntry(mappingConf);
	
		if(multiplexField != null){
			JSONArray jsonArray = documentContext.read(multiplexField);
			if(jsonArray == null){
				return;
			}
			
			for(int i=0; i< jsonArray.size();i++){
				bindValues(preparedStatementBean, documentContext, mappingConf, i, connection);	
			}
			
		}else{
			bindValues(preparedStatementBean, documentContext, mappingConf, null, connection);
		}
		
		logger.traceExit();
	}

	public static void bindValues(PreparedStatementBean preparedStatementBean, DocumentContext documentContext, String mappingConf, Integer loopCounter, Connection connection) throws Exception {
		int counter = 1;
		String[] mappings = mappingConf.split(",");
		for (String bindValue : preparedStatementBean.bindValueConfs) {

			String dataTypeStr = mappings[counter-1].trim();
            DataType dataType = DataType.valueOf(dataTypeStr.charAt(0)+"");
            try{
            	if(documentContext.read(bindValue) == null || documentContext.read(bindValue).toString().isEmpty()){
                	preparedStatementBean.preparedStatement.setObject(counter, null);
                	counter++;
                	continue;
                } 	
            }catch(PathNotFoundException p){
            	logger.warn(p.getMessage() +". Setting column to null");
            	preparedStatementBean.preparedStatement.setObject(counter, null);
            	counter++;
            	continue;
            }
            
            Method mapMethod = dataType.getMapperClass().getMethod("map", Object.class, String.class, Connection.class);
            Object value = null;
            
            if(loopCounter != null){
            	bindValue = bindValue.replaceAll("\\[\\*\\]", "["+loopCounter+"]");
            	value = mapMethod.invoke(null, documentContext.read(bindValue).toString(), dataTypeStr, connection);	
            }else{
            	value = mapMethod.invoke(null, documentContext.read(bindValue).toString(), dataTypeStr, connection);
            }
            
            logger.debug("Date Type "+dataType.name() +". Value "+ value);
           
            if(dataType.name().equals(DataType.D.toString())){
            	String outputTimeZone = DateDataTypeMapper.getConfigurationAtIndex(dataTypeStr, 2);
            	preparedStatementBean.preparedStatement.setDate(counter, (Date)value, Calendar.getInstance(TimeZone.getTimeZone(outputTimeZone)));
            }else{
            	preparedStatementBean.preparedStatement.setObject(counter, value);	
            }
            
			counter++;
		}
		preparedStatementBean.preparedStatement.addBatch();
	}
}
