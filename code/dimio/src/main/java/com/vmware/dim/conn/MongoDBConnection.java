package com.vmware.dim.conn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * Class to Manage the MongoDB Connection
 * 
 * @author ghimanshu
 *
 */
public class MongoDBConnection {

	private static final Logger logger = LogManager.getLogger(MongoDBConnection.class);
	private MongoClient mongoClient = null;

	public MongoDBConnection(String clientURI) {
		MongoClientURI mongoClientURI = new MongoClientURI(clientURI);
		mongoClient = new MongoClient(mongoClientURI);
	}

	public MongoClient getConnection() {
		logger.traceEntry();
		return logger.traceExit(mongoClient);
	}
}