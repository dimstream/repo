package com.vmware.common.processor;

import redis.clients.jedis.JedisCommands;

import com.jayway.jsonpath.DocumentContext;
import com.vmware.common.conn.RedisConnection;
import com.vmware.common.dim.DataTransformationUtil;
import com.vmware.common.dim.ScriptEngineCache;
import com.vmware.common.dto.ContextMappingOutputDTO;
import com.vmware.common.dto.PersistedFieldDTO;

public class PersistedFieldsProcessor {

	public static void process(DocumentContext streamDocumentContext, DocumentContext contextDocumentContext, ContextMappingOutputDTO contextMappingOutputDTO, String documentId) throws Exception {
		
		if (contextMappingOutputDTO.persistedFields != null) {
			JedisCommands jedis = RedisConnection.getConnection();
			
			for (PersistedFieldDTO persistedFieldDTO : contextMappingOutputDTO.persistedFields) {
				
				Object oldValue = ScriptEngineCache.read(contextDocumentContext, persistedFieldDTO.contextFieldName);
				if(oldValue != null){
					jedis.srem("Lookup_" + contextMappingOutputDTO.context.processContextName + "_" + DataTransformationUtil.getFieldName(persistedFieldDTO.contextFieldName) + "_" + oldValue.toString(), documentId);	
				}
				
				Object newValue = ScriptEngineCache.read(streamDocumentContext, persistedFieldDTO.streamFieldName);
				if(newValue != null){
					contextDocumentContext.set(persistedFieldDTO.contextFieldName, newValue);
					if(!contextMappingOutputDTO.context.singleStream){
						jedis.sadd("Lookup_" + contextMappingOutputDTO.context.processContextName + "_" + DataTransformationUtil.getFieldName(persistedFieldDTO.contextFieldName) + "_" + newValue.toString(), documentId);
					}
				}
			}
		}
		
	}
}
