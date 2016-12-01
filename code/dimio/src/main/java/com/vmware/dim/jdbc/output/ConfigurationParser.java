package com.vmware.dim.jdbc.output;

import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmware.common.dto.ConfigurationDetailsDTO;
import com.vmware.common.dto.ConfigurationPropertiesDTO;

public class ConfigurationParser {

	private static final Logger logger = LogManager.getLogger(ConfigurationParser.class);

	private Map<String, Map<String, String>> QUERY_CONFIG = new TreeMap<String, Map<String,String>>();
		
	public Map<String, Map<String, String>> get(){
		return QUERY_CONFIG;
	}
	
	public void init(ConfigurationDetailsDTO configurationDetailsDTO) throws Exception{
		if(QUERY_CONFIG.isEmpty()){
			parse(configurationDetailsDTO);	
		}
	}
	
	private void parse(ConfigurationDetailsDTO configurationDetailsDTO) throws Exception {
		logger.traceEntry();
		for(ConfigurationPropertiesDTO configurationPropertiesDTO : configurationDetailsDTO.configurationProperties){
			if(!configurationPropertiesDTO.configurationKey.startsWith("query")){
				continue;
			}
			
			String map1key = configurationPropertiesDTO.configurationKey.split("\\.")[0];
			if(QUERY_CONFIG.containsKey(map1key)){
				QUERY_CONFIG.get(map1key).put(configurationPropertiesDTO.configurationKey, configurationPropertiesDTO.configurationValue);
			}else{
				Map<String, String> innerMap = new TreeMap<String, String>();
				QUERY_CONFIG.put(map1key, innerMap);
				innerMap.put(configurationPropertiesDTO.configurationKey, configurationPropertiesDTO.configurationValue);
			}
		}
		logger.traceExit(QUERY_CONFIG);
	}
	
}
