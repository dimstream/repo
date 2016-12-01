package com.vmware.dim.jdbc.output;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class PreparedStatementCache {

	
	private Map<String, PreparedStatementBean> map = new HashMap<String, PreparedStatementBean>();
	
	public void add(String key, PreparedStatementBean preparedStatementBean){
		map.put(key, preparedStatementBean);
	}
	
	public PreparedStatementBean get(String key){
		return map.get(key);
	}
	
	public Collection<PreparedStatementBean> getValues(){
		return map.values();
	}
	
}
