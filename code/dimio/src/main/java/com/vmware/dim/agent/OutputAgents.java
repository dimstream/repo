package com.vmware.dim.agent;

import com.vmware.dim.impl.ElasticSearchOutputStream;
import com.vmware.dim.impl.HDFSOutputStream;
import com.vmware.dim.impl.JDBCOutputStream;
import com.vmware.dim.impl.KafkaOutputStream;
import com.vmware.dim.impl.MongoDBOutputStream;
import com.vmware.dim.impl.RMQOutputStream;
import com.vmware.dim.impl.WebSocketOutputStream;
import com.vmware.dim.output.OutputStreamWriter;

/**
 * Enum to classify the Output thread
 * 
 * @author ghimanshu
 *
 */
public enum OutputAgents {

	elasticsearch(ElasticSearchOutputStream.class),
	mongodb(MongoDBOutputStream.class),
	hdfs(HDFSOutputStream.class),
	rmq(RMQOutputStream.class),
	kafka(KafkaOutputStream.class),
	jdbc(JDBCOutputStream.class),
	websocket(WebSocketOutputStream.class);
	
	
	Class<? extends OutputStreamWriter> outputStreamClass;
	
	private OutputAgents(Class<? extends OutputStreamWriter> outputStreamClass) {
		this.outputStreamClass = outputStreamClass;
	}
	
	public Class<? extends OutputStreamWriter> getOutputStreamClass(){
		return this.outputStreamClass;
	}
}