package com.vmware.common.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.jayway.jsonpath.DocumentContext;
import com.vmware.common.conn.RedisConnection;
import com.vmware.common.dim.DataTransformationUtil;
import com.vmware.common.dim.ScriptEngineCache;
import com.vmware.common.dto.ComputedStateDTO;
import com.vmware.common.dto.ContextMappingOutputDTO;

import redis.clients.jedis.JedisCommands;

public class ComputedFieldsProcessor {
	
	private static final Logger logger = LogManager.getLogger(ComputedFieldsProcessor.class);

	public static void process(DocumentContext streamDocumentContext, DocumentContext contextDocumentContext, ContextMappingOutputDTO contextMappingOutputDTO, String documentId) throws Exception {
		
		if (contextMappingOutputDTO.computedStates != null) {
			JedisCommands jedis = RedisConnection.getConnection();
			for (ComputedStateDTO computedStateDTO : contextMappingOutputDTO.computedStates) {
				
				Object oldValue = ScriptEngineCache.read(contextDocumentContext, computedStateDTO.stateName);
				Object newValue = null;
				
				try{
					if(DataTransformationUtil.isExprForStream(computedStateDTO.conditions)){
						newValue = ScriptEngineCache.get().evalExprForComputedFields(streamDocumentContext, computedStateDTO.conditions);	
					}else{
						newValue = ScriptEngineCache.get().evalExprForComputedFields(contextDocumentContext, computedStateDTO.conditions);
					}
				}catch(Exception exception){
					logger.error("Unable to set the value for field " + computedStateDTO.stateName, exception);
				}
				
				if(newValue != null){
					contextDocumentContext.set(computedStateDTO.stateName, newValue);
					if(oldValue != null){
						jedis.srem("Lookup_" + contextMappingOutputDTO.context.processContextName + "_" + DataTransformationUtil.getFieldName(computedStateDTO.stateName) + "_" + oldValue.toString(), documentId);	
					}
					if(!contextMappingOutputDTO.context.singleStream){
						jedis.sadd("Lookup_" + contextMappingOutputDTO.context.processContextName + "_" + DataTransformationUtil.getFieldName(computedStateDTO.stateName) + "_" + newValue.toString(), documentId);
					}
				}
			}
		}
	}
}
