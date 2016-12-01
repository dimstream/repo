package com.vmware.dim.agent;

import com.vmware.dim.impl.KafkaInputStream;
import com.vmware.dim.impl.RMQInputStream;
import com.vmware.dim.impl.SalesforceInputStream;
import com.vmware.dim.impl.SOQLInputStream;
import com.vmware.dim.impl.WebSocketInputStream;
import com.vmware.dim.input.InputStreamReader;

/**
 * Enum to classify the Input thread
 * 
 * @author ghimanshu
 *
 */
public enum InputAgents {

	rmq(RMQInputStream.class),
	websocket(WebSocketInputStream.class),
	soql(SOQLInputStream.class),
	kafka(KafkaInputStream.class),
	sfstreaming(SalesforceInputStream.class);
	
	Class<? extends InputStreamReader> inputStreamClass;
	
	private InputAgents(Class<? extends InputStreamReader> inputStreamClass) {
		this.inputStreamClass = inputStreamClass;
	}
	
	public Class<? extends InputStreamReader> getInputStreamClass(){
		return this.inputStreamClass;
	}
}
